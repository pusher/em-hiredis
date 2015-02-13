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
      # first connect call
      [ :initial, :connecting ],
      # TCP connect fails
      [ :connecting, :disconnected ],
      # TCP connection up, need to auth and select db before processing any commands
      [ :connecting, :setting_up ],
      # auth and db select do not succeed (rejected, connection failure, etc...)
      [ :setting_up, :disconnected ],
      # connection is ready to process commands
      [ :setting_up, :connected ],
      # connection lost
      [ :connected, :disconnected ],
      # attempting automatic reconnect
      [ :disconnected, :connecting ],
      # all automatic reconnection attempts failed
      [ :disconnected, :failed ],
      # manual call of reconnect after failure
      [ :failed, :connecting ],
    ]

    def initialize(uri)
      configure(uri)

      @reconnect_attempt = 0

      # Commands received while we are not initialized, to be sent once we are
      @command_queue = []

      @sm = StateMachine.new
      TRANSITIONS.each { |t| @sm.transition(*t) }

      @sm.on(:connecting, &method(:connect_internal))
      @sm.on(:setting_up, &method(:setup))
      @sm.on(:connected, &method(:connected))
      @sm.on(:disconnected, &method(:disconnected))
      @sm.on(:failed, &method(:perm_failure))
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

    # For overriding by tests to inject mock connections and avoid eventmachine
    def em_connect
      EM.connect(@host, @port, EMReqRespConnection)
    end

    def em_timer(delay, &blk)
      EM.add_timer(delay, &blk)
    end

    private

    def connect_internal(prev_state)
      begin
        @connection = em_connect
        @connection.on(:connected) {
          @sm.update_state(:setting_up)
        }
        @connection.on(:disconnected) {
          @sm.update_state(:disconnected)
        }
      rescue EventMachine::ConnectionError => e
        puts e
        @sm.update_state(:disconnected)
      end
    end

    def maybe_reconnect(delay = false)
      emit(:reconnect_failed, @reconnect_attempt) if @reconnect_attempt > 0

      if @reconnect_attempt > 3
        @sm.update_state(:failed)
      else
        @reconnect_attempt += 1
        if delay == :delayed
          @reconnect_timer = em_timer(EventMachine::Hiredis.reconnect_timeout) {
            @reconnect_timer = nil
            @sm.update_state(:connecting)
          }
        elsif delay == :immediate
          @sm.update_state(:connecting)
        else
          raise "Unrecognised delay specifier #{delay}"
        end
      end
    end

    def setup(prev_state)
      maybe_auth.callback {
        maybe_select.callback {
          @sm.update_state(:connected)
        }.errback { |e|
          # Failure to select db counts as a connection failure
          @connection.close_connection
        }
      }.errback { |e|
        # Failure to auth counts as a connection failure
        @connection.close_connection
      }
    end

    def connected(prev_state)
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

    def perm_failure(prev_state)
      emit(:failed)
      set_deferred_status(:failed, EM::Hiredis::Error.new('Could not connect after 4 attempts'))

      @command_queue.each { |df, command, args|
        df.fail(EM::Hiredis::Error.new('Redis connection in failed state'))
      }
      @command_queue.clear
    end

    def disconnected(prev_state)
      delay = case prev_state
      when :connected
        emit(:disconnected)
        :immediate
      when :connecting
        :delayed
      when :setting_up
        :delayed
      end

      maybe_reconnect(delay)
    end

    def process_command(command, *args, &blk)
      puts "process command #{command}"

      df = EM::DefaultDeferrable.new
      # Shortcut for defining the callback case with just a block
      df.callback(&blk) if blk

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

    def maybe_auth
      if @password
        @connection.send_command(EM::DefaultDeferrable.new, 'auth', @password)
      else
        noop
      end
    end

    def maybe_select
      if @db != 0
        @connection.send_command(EM::DefaultDeferrable.new, 'select', @db)
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
