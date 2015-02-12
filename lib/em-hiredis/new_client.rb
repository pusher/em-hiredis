require 'uri'

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  class NewClient
    include EventEmitter

    def initialize(host = 'localhost', port = 6379, password = nil, db = 0)
      @host, @port, @password, @db = host, port, password, db

      @reconnect_attempt = 0

      # returned from `connect_internal`, this succeeds or fails with the current
      # connection attempt
      @connected_deferrable = nil

      # returned from `connect`, this will only fail once retries have been
      # exhausted, abstracting connection retries from the public
      @public_connected_deferrable = nil

      # Not just that we are connected, but that we have e.g. selected db
      # and are ready to process other commands on the connection
      @initialized = false

      # We have failed as many automated reconnects as we are willing to try,
      # and won't be usable again until `connect` is called from outside.
      @failed = false

      # Commands received while we are not initialized, to be sent once we are
      @command_queue = []
    end

    def connect
      puts "connect"
      connect_internal
      return @public_connected_deferrable = EM::DefaultDeferrable.new
    end

    def reconnect
      puts "reconnect"
      if @connection
        @connection.close_connection
      else
        connect
      end
    end

    protected

    # For overriding by tests to inject mock connections and avoid eventmachine
    def em_connect
      EM.connect(@host, @port, EMReqRespConnection)
    end

    private

    def connect_internal
      puts "connect_internal"

      @failed = false
      @connected_deferrable = df = EM::DefaultDeferrable.new

      begin
        @connection = em_connect
        @connection.on(:connected) {
          on_connection_complete
        }
        @connection.on(:disconnected) {
          on_disconnected
        }
      rescue EventMachine::ConnectionError => e
        # fails @connected_deferrable
        puts e
        on_disconnected
      end

      return df
    end

    def reconnect_internal
      puts "reconnect_internal"
      emit(:reconnect_failed, @reconnect_attempt) if @reconnect_attempt > 0

      if @reconnect_attempt > 3
        @failed = true
        if @public_connected_deferrable
          @public_connected_deferrable.fail
          @public_connected_deferrable = nil
        end
        emit(:failed)
      else
        @reconnect_attempt += 1
        connect_internal.callback {
          @reconnect_attempt == 0
          emit(:reconnected)
        }
      end
    end

    def on_connection_complete
      puts "on_connection_complete"
      @connection.send_command(EM::DefaultDeferrable.new, 'select', @db).callback {
        on_initialisation_complete
      }.errback { |e|
        # Failure to select db counts as a connection failure
        @connection.close_connection
      }
    end

    def on_initialisation_complete
      puts "on_initialisation_complete"
      emit(:connected)
      if @connected_deferrable
        @connected_deferrable.succeed
        @connected_deferrable = nil
      end

      if @public_connected_deferrable
        @public_connected_deferrable.succeed
        @public_connected_deferrable = nil
      end

      @initialized = true
      puts "Command queue size: #{@command_queue.size}"
      @command_queue.each { |df, command, args|
        @connection.send_command(df, command, args)
      }
      @command_queue.clear
    end

    def on_disconnected
      puts "on_disconnected"
      @initialized = false

      if @connected_deferrable
        @connected_deferrable.fail
        @connected_deferrable = nil
      end

      reconnect_internal
    end

    def process_command(command, *args)
      puts "process command #{command}"

      df = EM::DefaultDeferrable.new
      # Shortcut for defining the callback case with just a block
      df.callback { |result| yield(result) } if block_given?

      if @failed
        df.fail(EM::Hiredis::RedisError.new('Connection in failed state'))
      elsif @initialized
        @connection.send_command(df, command, args)
      else
        @command_queue << [df, command, args]
      end

      return df
    end

    alias_method :method_missing, :process_command

  end
end
