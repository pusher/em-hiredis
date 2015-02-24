require 'uri'

module EventMachine::Hiredis
  # Emits the following events
  #
  # * :connected - on successful connection or reconnection
  # * :reconnected - on successful reconnection
  # * :disconnected - no longer connected, when previously in connected state
  # * :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  class PubsubClient
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

      # Subscribes received while we are not initialized, to be sent once we are
      @command_queue = []

      @connection_manager = ConnectionManager.new(em, method(:factory_connection))

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

    def connect
      @connection_manager.connect
      return self
    end

    def reconnect(uri = nil)
      configure(uri) if uri
      @connection_manager.reconnect
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

    def configure(uri_string)
      uri = URI(uri_string)

      @host = uri.host
      @port = uri.port
      @password = uri.password
    end

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

            connection.on(:message, &method(:message_callbacks))
            connection.on(:pmessage, &method(:pmessage_callbacks))

            [ :message,
              :pmessage,
              :subscribe,
              :unsubscribe,
              :psubscribe,
              :punsubscribe
            ].each do |command|
              connection.on(command) { |*args|
                emit(command, *args)
              }
            end

            @command_queue.each { |df, command, args|
              connection.send_command(df, command, args)
            }
            @command_queue.clear

            @subscriptions.keys.each do |channel|
              @connection_manager.connection.send_command(
                EM::DefaultDeferrable.new,
                :subscribe,
                channel
              )
            end
            @psubscriptions.keys.each do |pattern|
              @connection_manager.connection.send_command(
                EM::DefaultDeferrable.new,
                :psubscribe,
                pattern
              )
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
      df = EM::DefaultDeferrable.new

      if subscriptions.include?(channel)
        # Short circuit issuing the command if we're already subscribed
        subscriptions[channel] << cb
        df.succeed
      elsif @connection_manager.state == :failed
        df.fail('Redis connection in failed state')
      elsif @connection_manager.state == :connected
        @connection_manager.connection.send_command(df, type, channel)
        df.callback {
          subscriptions[channel] << cb
        }
      else
        @command_queue << [df, type, channel]
      end

      return df
    end

    def unsubscribe_impl(type, subscriptions, channel)
      if subscriptions.include?(channel)
        subscriptions.delete(channel)
        if @connection_manager.state == :connected
          @connection_manager.connection.send_command(EM::DefaultDeferrable.new, type, channel)
        else
          noop
        end
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

    def maybe_auth(connection)
      if @password
        connection.send_command(EM::DefaultDeferrable.new, 'auth', @password)
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
