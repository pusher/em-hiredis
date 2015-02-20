require 'uri'

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  class BaseClient
    include EventEmitter
    include EventMachine::Deferrable

    attr_reader :host, :port, :password, :db

    def initialize(uri, em = EventMachine)
      @em = em
      configure(uri)

      # Number of seconds of inactivity on a connection before it sends a ping
      @inactivity_trigger_secs = 0
      # Number of seconds of further inactivity after a ping is sent before
      # the connection is considered failed
      @inactivity_response_timeout = 0

      # Commands received while we are not initialized, to be sent once we are
      @command_queue = []

      @client_state_machine = ClientStateMachine.new(em, method(:factory_connection))

      @client_state_machine.on(:connected) {
        emit(:connected)
        set_deferred_status(:succeeded)
      }

      @client_state_machine.on(:disconnected) { emit(:disconnected) }
      @client_state_machine.on(:reconnected) { emit(:reconnected) }
      @client_state_machine.on(:reconnect_failed) { |count| emit(:reconnect_failed, count) }

      @client_state_machine.on(:failed) {
        emit(:failed)
        set_deferred_status(:failed, Error.new('Could not connect after 4 attempts'))
      }
    end

    def configure(uri_string)
      uri = URI(uri_string)

      path = uri.path[1..-1]
      db = path.to_i # Empty path => 0

      @host = uri.host
      @port = uri.port
      @password = uri.password
      @db = db
    end

    def connect
      @client_state_machine.connect
      self
    end

    def reconnect
      @client_state_machine.reconnect
    end

    def configure_inactivity_check(trigger_secs, response_timeout)
      raise ArgumentError('trigger_secs must be > 0') unless trigger_secs.to_i > 0
      raise ArgumentError('response_timeout must be > 0') unless response_timeout.to_i > 0

      @inactivity_trigger_secs = trigger_secs.to_i
      @inactivity_response_timeout = response_timeout.to_i
    end

    ## Commands which require extra logic

    def select(db, &blk)
      process_command('select', db, &blk).callback {
        @db = db
      }
    end

    def auth(password, &blk)
      process_command('auth', db, &blk).callback {
        @password = password
      }
    end

    protected

    def factory_connection
      df = EM::DefaultDeferrable.new

      begin
        connection = @em.connect(@host, @port, ReqRespConnection)

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

    def connected(prev_state)
      set_deferred_status(:succeeded)
    end

    def perm_failure(prev_state)
      set_deferred_status(:failed, EM::Hiredis::Error.new('Could not connect after 4 attempts'))

      @command_queue.each { |df, command, args|
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      }
      @command_queue.clear
    end

    def process_command(command, *args, &blk)
      puts "process command #{command}"

      df = EM::DefaultDeferrable.new
      # Shortcut for defining the callback case with just a block
      df.callback(&blk) if blk

      if @client_state_machine.state == :failed
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      elsif @client_state_machine.state == :connected
        @client_state_machine.connection.send_command(df, command, args)
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
