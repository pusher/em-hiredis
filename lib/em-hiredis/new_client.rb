require 'uri'

module EventMachine::Hiredis
  # Emits the following events:
  #
  # - :connected - on successful connection or reconnection
  # - :reconnected - on successful reconnection
  # - :disconnected - no longer connected, when previously in connected state
  # - :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  # - :failed - on failing the final reconnect attempt
  class BaseClient
    include EventEmitter
    include EventMachine::Deferrable

    attr_reader :host, :port, :password, :db

    # uri:
    #   the redis server to connect to, redis://[:password@]host[:port][/db]
    # inactivity_trigger_secs:
    #   the number of seconds of inactivity before triggering a ping to the server
    # inactivity_response_timeout:
    #   the number of seconds after a ping at which to terminate the connection
    #   if there is still no activity
    def initialize(
        uri,
        inactivity_trigger_secs = nil,
        inactivity_response_timeout = nil,
        em = EventMachine)

      @em = em
      configure(uri)

      # Number of seconds of inactivity on a connection before it sends a ping
      @inactivity_trigger_secs = inactivity_trigger_secs
      # Number of seconds of further inactivity after a ping is sent before
      # the connection is considered failed
      @inactivity_response_timeout = inactivity_response_timeout

      # Commands received while we are not initialized, to be sent once we are
      @command_queue = []

      @connection_manager = ConnectionManager.new(method(:factory_connection), em)

      @connection_manager.on(:connected) {
        emit(:connected)
        set_deferred_status(:succeeded)
      }

      @connection_manager.on(:disconnected) { emit(:disconnected) }
      @connection_manager.on(:reconnected) { emit(:reconnected) }
      @connection_manager.on(:reconnect_failed) { |count| emit(:reconnect_failed, count) }

      @connection_manager.on(:failed) {
        @command_queue.each { |df, _, _|
          df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
        }
        @command_queue.clear

        emit(:failed)
        set_deferred_status(:failed, Error.new('Could not connect after 4 attempts'))
      }
    end

    # Connect to the configured redis server. Returns a deferrable which
    # completes upon successful connections or fails after all reconnect attempts
    # are exhausted.
    #
    # Commands may be issued before or during connection, they will be queued
    # and submitted to the server once the connection is active.
    def connect
      @connection_manager.connect
      return self
    end

    # Reconnect, either:
    #  - because the client has reached a failed state, but you believe the
    #    underlying problem to be resolved
    #  - with an optional different uri, because you wish to tear down the
    #    connection and connect to a different redis server, perhaps as part of
    #    a failover
    def reconnect(uri = nil)
      configure(uri) if uri
      @connection_manager.reconnect
    end

    ## Commands which require extra logic

    def select(db, &blk)
      process_command('select', db, &blk).callback {
        @db = db
      }
    end

    def auth(password, &blk)
      process_command('auth', password, &blk).callback {
        @password = password
      }
    end

    protected

    def configure(uri_string)
      uri = URI(uri_string)

      path = uri.path[1..-1]
      db = path.to_i # Empty path => 0

      @host = uri.host
      @port = uri.port
      @password = uri.password
      @db = db
    end

    def factory_connection
      df = EM::DefaultDeferrable.new

      begin
        connection = @em.connect(
          @host,
          @port,
          ReqRespConnection,
          @inactivity_trigger_secs,
          @inactivity_response_timeout
        )

        connection.on(:connected) {
          maybe_auth(connection).callback {
            maybe_select(connection).callback {
              @command_queue.each { |df, command, args|
                connection.send_command(df, command, args)
              }
              @command_queue.clear

              df.succeed(connection)
            }.errback { |e|
              # Failure to select db counts as a connection failure
              connection.close_connection
              df.fail(e)
            }
          }.errback { |e|
            # Failure to auth counts as a connection failure
            connection.close_connection
            df.fail(e)
          }
        }

        connection.on(:connection_failed) {
          df.fail('Connection failed')
        }
      rescue EventMachine::ConnectionError => e
        df.fail(e)
      end

      return df
    end

    def process_command(command, *args, &blk)
      df = EM::DefaultDeferrable.new
      # Shortcut for defining the callback case with just a block
      df.callback(&blk) if blk

      if @connection_manager.state == :failed
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      elsif @connection_manager.state == :connected
        @connection_manager.connection.send_command(df, command, args)
      else
        @command_queue << [df, command, args]
      end

      return df
    end

    alias_method :method_missing, :process_command

    def maybe_auth(connection)
      if @password
        connection.send_command(EM::DefaultDeferrable.new, 'auth', @password)
      else
        noop
      end
    end

    def maybe_select(connection)
      if @db != 0
        connection.send_command(EM::DefaultDeferrable.new, 'select', @db)
      else
        noop
      end
    end

    def noop
      df = EM::DefaultDeferrable.new
      df.succeed
      df
    end
  end
end
