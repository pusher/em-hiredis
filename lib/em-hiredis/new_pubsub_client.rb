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

    def initialize(uri)
      super

      # Subscribed channels to their callbacks
      @subscriptions = Hash.new { |h, k| h[k] = [] }
      # Subscribed patterns to their callbacks
      @psubscriptions = Hash.new { |h, k| h[k] = [] }

      on(:connected) {
        @subscriptions.keys.each { |channel| process_command(:subscribe, channel) }
        @psubscriptions.keys.each { |pattern| process_command(:psubscribe, pattern) }
      }
    end

    def configure(uri_string)
      super
      @db = 0 # pubsub operates outside the tablespace
    end

    ## Commands which require extra logic

    def select(db, &blk)
      # Pubsub operates outside the tablespace
      noop
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

    # For overriding by tests to inject mock connections and avoid eventmachine
    def em_connect
      EM.connect(@host, @port, EMPubsubConnection)
    end

    def connect_internal(prev_state)
      super

      @connection.on(:message) { |channel, message|
        message_callbacks(channel, message)
      }
      @connection.on(:pmessage) { |pattern, channel, message|
        pmessage_callbacks(pattern, channel, message)
      }

      [ :subscribe, :unsubscribe, :psubscribe, :punsubscribe ].each do |command|
        @connection.on(command) { |args|
          emit(command, args)
        }
      end
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
      emit(:message, channel, message)
      cbs = @subscriptions[channel]
      if cbs
        cbs.each { |cb| cb.call(message) if cb }
      end
    end

    def pmessage_callbacks(pattern, channel, message)
      emit(:pmessage, pattern, channel, message)
      cbs = @psubscriptions[pattern]
      if cbs
        cbs.each { |cb| cb.call(channel, message) if cb }
      end
    end
  end
end
