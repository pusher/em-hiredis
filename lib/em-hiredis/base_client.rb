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

    TRANSITIONS = [
      [ :connect,                 :initial, :connecting ],
      [ :connect_failure,         :connecting, :connect_failed ],
      [ :retry_connect,           :connect_failed, :connecting ],
      [ :connect_perm_failure,    :connect_failed, :failed ],
      [ :initialise,              :connecting, :initialising ],
      [ :initialise_failure,      :initialising, :initialise_failed ],
      [ :retry_initialise,        :initialise_failed, :connecting ],
      [ :initialise_perm_failure, :initialise_failed, :failed ],
      [ :initialise_success,      :initialising, :connected ],
      [ :disconnected,            :connected, :disconnected ],
      [ :reconnect,               :disconnected, :connecting ],
      [ :recover,                 :failed, :connecting ],
    ]

    def initialize(uri)
      configure(uri)

      @reconnect_attempt = 0

      # Commands received while we are not initialized, to be sent once we are
      @command_queue = []

      @sm = StateMachine.new
      TRANSITIONS.each { |t| @sm.add_transition(*t) }

      @sm.on(:connect) { connect_internal }
      @sm.on(:retry_connect) { connect_internal }
      @sm.on(:reconnect) { connect_internal }
      @sm.on(:recover) { connect_internal }
      @sm.on(:retry_initialise) { connect_internal }

      @sm.on(:connect_failure) { maybe_reconnect }

      @sm.on(:initialise) { initialise }
      @sm.on(:initialise_success) { initialise_success }
      @sm.on(:initialise_failure) { initialise_failure }

      @sm.on(:connect_perm_failure) { perm_failure }
      @sm.on(:initialise_perm_failure) { perm_failure }

      @sm.on(:disconnected) { disconnected }
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
      @sm.update_state(:connecting)

      @deferred_status = nil
      return self
    end

    def reconnect
      if @connection
        @connection.close_connection
      else
        connect
      end
    end

    ## Commands which require extra logic

    def select(db, &blk)
      @db = db
      process_command('select', db, &blk)
    end

    def auth(password, &blk)
      @password = password
      process_command('auth', db, &blk)
    end

    protected

    # For overriding by tests to inject mock connections and avoid eventmachine
    def em_connect
      EM.connect(@host, @port, EMReqRespConnection)
    end

    def em_timer(delay, &blk)
      EM.add_timer(delay, &blk)
    end

    private

    def connect_internal
      begin
        @connection = em_connect
        @connection.on(:connected) {
          @sm.update_state(:initialising)
        }
        @connection.on(:connection_failed) {
          @sm.update_state(:connect_failed)
        }
        @connection.on(:disconnected) {
          @sm.update_state(:disconnected)
        }
      rescue EventMachine::ConnectionError => e
        puts e
        @sm.update_state(:connect_failed)
      end
    end

    def maybe_reconnect
      emit(:reconnect_failed, @reconnect_attempt) if @reconnect_attempt > 0

      if @reconnect_attempt > 3
        @sm.update_state(:failed)
      else
        @reconnect_attempt += 1
        @reconnect_timer = em_timer(EventMachine::Hiredis.reconnect_timeout) {
          @reconnect_timer = nil
          @sm.update_state(:connecting)
        }
      end
    end

    def initialise
      maybe_auth.callback {
        maybe_select.callback {
          @sm.update_state(:connected)
        }.errback { |e|
          # Failure to select db counts as a connection failure
          @sm.update_state(:initialise_failed)
        }
      }.errback { |e|
        # Failure to auth counts as a connection failure
        @sm.update_state(:initialise_failed)
      }
    end

    def maybe_auth
      if @password
        @connection.send_command(EM::DefaultDeferrable.new, 'auth', @password)
      else
        df = EM::DefaultDeferrable.new
        df.succeed
        df
      end
    end

    def maybe_select
      if @db != 0
        @connection.send_command(EM::DefaultDeferrable.new, 'select', @db)
      else
        df = EM::DefaultDeferrable.new
        df.succeed
        df
      end
    end

    def initialise_success
      emit(:connected)
      if @reconnect_attempt > 0
        emit(:reconnected)
        @reconnect_attempt = 0
      end

      set_deferred_status(:succeeded)

      @command_queue.each { |df, command, args|
        @connection.send_command(df, command, args)
      }
      @command_queue.clear
    end

    def initialise_failure
      # Close the "failed" connection, but first unsubscribe from its eventemitter
      # because we are treating it as "already closed"
      @connection.remove_all_listeners(:disconnected)
      @connection.close_connection

      maybe_reconnect
    end

    def perm_failure
      @command_queue.each { |df, command, args|
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      }
      @command_queue.clear

      set_deferred_status(:failed, EM::Hiredis::Error.new('Could not connect after 4 attempts'))
      emit(:failed)
    end

    def disconnected
      emit(:disconnected)
      maybe_reconnect
    end

    def process_command(command, *args)
      puts "process command #{command}"

      df = EM::DefaultDeferrable.new
      # Shortcut for defining the callback case with just a block
      df.callback { |result| yield(result) } if block_given?

      if @sm.state == :failed
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      elsif @sm.state == :connected
        @connection.send_command(df, command, args)
      else
        @command_queue << [df, command, args]
      end

      return df
    end

    alias_method :method_missing, :process_command

  end
end
