require 'uri'

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  class PubsubClient < BaseClient
    include EventEmitter
    include EventMachine::Deferrable

    attr_reader :host, :port, :password

    def initialize(
        uri,
        inactivity_trigger_secs = nil,
        inactivity_response_timeout = nil,
        em = EventMachine)

      @em = em
      configure(uri)

      # Number of seconds of inactivity on a connection before it sends a ping
      @inactivity_trigger_secs = if inactivity_trigger_secs
        raise ArgumentError('inactivity_trigger_secs must be > 0') unless inactivity_trigger_secs.to_i > 0
        inactivity_trigger_secs.to_i
      end

      # Number of seconds of further inactivity after a ping is sent before
      # the connection is considered failed
      @inactivity_response_timeout = if inactivity_response_timeout
        raise ArgumentError('inactivity_response_timeout must be > 0') unless inactivity_response_timeout.to_i > 0
        inactivity_response_timeout.to_i
      end

      # Subscribed channels to their callbacks
      @subscriptions = Hash.new { |h, k| h[k] = [] }
      # Subscribed patterns to their callbacks
      @psubscriptions = Hash.new { |h, k| h[k] = [] }

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

      @host = uri.host
      @port = uri.port
      @password = uri.password
    end

    def subscribe(channel, proc = nil, &blk)
      cb = proc || blk
      subscribe_impl(:subscribe, @subscriptions, channel, cb)
    end

    def psubscribe(pattern, proc = nil, &blk)
      cb = proc || blk
      subscribe_impl(:psubscribe, @psubscriptions, pattern, cb)
    end

    def unsubscribe(channel)
      unsubscribe_impl(:unsubscribe, @subscriptions, channel)
    end

    def punsubscribe(pattern)
      unsubscribe_impl(:punsubscribe, @psubscriptions, pattern)
    end

    def unsubscribe_proc(channel, proc)
      unsubscribe_proc_impl(:unsubscribe, @subscriptions, channel, proc)
    end

    def punsubscribe_proc(pattern, proc)
      unsubscribe_proc_impl(:punsubscribe, @psubscriptions, pattern, proc)
    end

    protected

    def factory_connection
      df = EM::DefaultDeferrable.new

      begin
        connection = @em.connect(
          @host,
          @port,
          PubsubConnection,
          @inactivity_trigger_secs,
          @inactivity_response_timeout
        )

        connection.on(:connected) {
          maybe_auth(connection).callback {
            @subscriptions.keys.each { |channel| process_command(:subscribe, channel) }
            @psubscriptions.keys.each { |pattern| process_command(:psubscribe, pattern) }

            connection.on(:message, &method(:message_callbacks))
            connection.on(:pmessage, &method(:pmessage_callbacks))

            [ :message,
              :pmessage,
              :subscribe,
              :unsubscribe,
              :psubscribe,
              :punsubscribe
            ].each do |command|
              connection.on(command) { |args|
                emit(command, args)
              }
            end

            df.succeed(connection)
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

    def subscribe_impl(type, subscriptions, channel, cb)
      if subscriptions.include?(channel)
        subscriptions[channel] << cb
        return noop
      else
        df = process_command(type, channel)
        return df.callback {
          subscriptions[channel] << cb
        }
      end
    end

    def unsubscribe_impl(type, subscriptions, channel)
      if subscriptions.include?(channel)
        subscriptions.delete(channel)
        process_command(type, channel)
      end
    end

    def unsubscribe_proc_impl(type, subscriptions, channel, proc)
      df = EM::DefaultDeferrable.new
      if subscriptions[channel].delete(proc)
        if subscriptions[channel].any?
          # Succeed deferrable immediately - no need to unsubscribe
          df.succeed
        else
          unsubscribe_impl(type, subscriptions, channel).callback {
            df.succeed
          }
        end
      else
        df.fail
      end
      return df
    end

    def message_callbacks(channel, message)
      cbs = @subscriptions[channel]
      if cbs
        cbs.each { |cb| cb.call(message) if cb }
      end
    end

    def pmessage_callbacks(pattern, channel, message)
      cbs = @psubscriptions[pattern]
      if cbs
        cbs.each { |cb| cb.call(channel, message) if cb }
      end
    end
  end
end
