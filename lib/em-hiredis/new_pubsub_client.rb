require 'uri'

module EventMachine::Hiredis
  # Emits the following events:
  #
  # Life cycle events:
  # - :connected - on successful connection or reconnection
  # - :reconnected - on successful reconnection
  # - :disconnected - no longer connected, when previously in connected state
  # - :reconnect_failed(failure_number) - a reconnect attempt failed
  #     This event is passed number of failures so far (1,2,3...)
  # - :failed - on failing the final reconnect attempt
  #
  # Subscription events:
  # - :message(channel, message) - on receiving message on channel which has an active subscription
  # - :pmessage(pattern, channel, message) - on receiving message on channel which has an active subscription due to pattern
  # - :subscribe(channel) - on confirmation of subscription to channel
  # - :psubscribe(pattern) - on confirmation of subscription to pattern
  # - :unsubscribe(channel) - on confirmation of unsubscription from channel
  # - :punsubscribe(pattern) - on confirmation of unsubscription from pattern
  #
  # Note that :subscribe and :psubscribe will be emitted after a reconnection for
  # all subscriptions which were active at the time the connection was lost.
  class PubsubClient
    include EventEmitter
    include EventMachine::Deferrable

    attr_reader :host, :port, :password

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

            @subscriptions.keys.each do |channel|
              connection.send_command(
                :subscribe,
                channel
              )
            end
            @psubscriptions.keys.each do |pattern|
              connection.send_command(
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
      if subscriptions.include?(channel)
        # Short circuit issuing the command if we're already subscribed
        subscriptions[channel] << cb
      elsif @connection_manager.state == :failed
        # TODO this is not an OK way of signally failure
        raise('Redis connection in failed state')
      elsif @connection_manager.state == :connected
        @connection_manager.connection.send_command(type, channel)
        subscriptions[channel] << cb
      else
        # We will issue subscription command when we connect
        subscriptions[channel] << cb
      end
    end

    def unsubscribe_impl(type, subscriptions, channel)
      if subscriptions.include?(channel)
        subscriptions.delete(channel)
        if @connection_manager.state == :connected
          @connection_manager.connection.send_command(type, channel)
        end
      end
    end

    def unsubscribe_proc_impl(type, subscriptions, channel, proc)
      removed = subscriptions[channel].delete(proc)

      # Kill the redis subscription if that was the last callback
      if removed && subscriptions[channel].empty?
        unsubscribe_impl(type, subscriptions, channel)
      end
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
        connection.auth(@password)
      else
        df = EM::DefaultDeferrable.new
        df.succeed
        df
      end
    end
  end
end
