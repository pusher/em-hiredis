require 'uri'

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  class ConnectionManager
    include EventEmitter

    TRANSITIONS = [
      # first connect call
      [ :initial, :connecting ],
      # TCP connect fails
      [ :connecting, :disconnected ],
      [ :connecting, :connected ],
      # connection lost
      [ :connected, :disconnected ],
      # attempting automatic reconnect
      [ :disconnected, :connecting ],
      # all automatic reconnection attempts failed
      [ :disconnected, :failed ],
      # manual call of reconnect after failure
      [ :failed, :connecting ],
    ]

    def initialize(em, connection_factory)
      @em = em
      @connection_factory = connection_factory

      @reconnect_attempt = 0

      @sm = StateMachine.new
      TRANSITIONS.each { |t| @sm.transition(*t) }

      @sm.on(:connecting, &method(:connect_internal))
      @sm.on(:connected, &method(:connected))
      @sm.on(:disconnected, &method(:disconnected))
      @sm.on(:failed, &method(:perm_failure))
    end

    def connect
      @sm.update_state(:connecting)
    end

    def reconnect
      if @connection
        @connection.close_connection
      else
        connect
      end
    end

    def state
      @sm.state
    end

    def connection
      @connection
    end

    protected

    def connect_internal(prev_state)
      if @reconnect_timer
        @em.cancel_timer(@reconnect_timer)
        @reconnect_timer = nil
      end

      @connection_factory.call.callback { |connection|
        @connection = connection
        @sm.update_state(:connected)

        @connection.on(:disconnected) {
          @sm.update_state(:disconnected)
        }
      }.errback {
        @sm.update_state(:disconnected)
      }
    end

    def connected(prev_state)
      emit(:connected)
      if @reconnect_attempt > 0
        emit(:reconnected)
        @reconnect_attempt = 0
      end

      set_deferred_status(:succeeded)
    end

    def perm_failure(prev_state)
      emit(:failed)
    end

    def disconnected(prev_state)
      delay = case prev_state
      when :connected
        emit(:disconnected)
        :immediate
      when :connecting
        :delayed
      end

      emit(:reconnect_failed, @reconnect_attempt) if @reconnect_attempt > 0

      if @reconnect_attempt > 3
        @sm.update_state(:failed)
      else
        @reconnect_attempt += 1
        if delay == :delayed
          @reconnect_timer = @em.add_timer(EventMachine::Hiredis.reconnect_timeout) {
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
  end
end
