module EventMachine::Hiredis
  module PubsubConnection
    include EventMachine::Hiredis::EventEmitter

    PUBSUB_COMMANDS = %w{subscribe unsubscribe psubscribe punsubscribe}.freeze
    PUBSUB_MESSAGES = %w{message pmessage}.freeze

    PING_CHANNEL = '__em-hiredis-ping'

    def initialize(inactivity_trigger_secs = nil, inactivity_response_timeout = 2)
      @reader = ::Hiredis::Reader.new
      @response_queues = Hash.new { |h, k| h[k] = [] }
    end

    def send_command(df, command, channel)
      if PUBSUB_COMMANDS.include?(command.to_s)
        @response_queues[channel] << df
        send_data(marshal(command, channel))
        return df
      else
        raise "Cannot send command '#{command}' on Pubsub connection"
      end
    end

    # EM::Connection callback
    def connection_completed
      @connected = true
      emit(:connected)

      schedule_inactivity_checks if @inactivity_trigger_secs
    end

    # EM::Connection callback
    def receive_data(data)
      @inactive_seconds = 0

      @reader.feed(data)
      until (reply = @reader.gets) == false
        puts "reply #{reply}"
        handle_response(reply)
      end
    end

    protected

    COMMAND_DELIMITER = "\r\n"

    def marshal(*args)
      command = []
      command << "*#{args.size}"

      args.each do |arg|
        arg = arg.to_s
        command << "$#{arg.to_s.bytesize}"
        command << arg
      end

      command.join(COMMAND_DELIMITER) + COMMAND_DELIMITER
    end

    def handle_response(reply)
      type = reply[0]
      if PUBSUB_COMMANDS.include?(type)
        _, channel, sub_count = reply
        df = @response_queues[channel].pop
        df.succeed(sub_count)

        if @response_queues[channel].empty?
          @response_queues.delete(channel)
        end
        emit(type.to_sym, *reply[1..-1])
      elsif PUBSUB_MESSAGES.include?(type)
        emit(type.to_sym, *reply[1..-1])
      else
        raise "Unrecognised response #{reply}"
      end
    end

    def ping
      send_command(EM::DefaultDeferrable.new, 'subscribe', [PING_CHANNEL]).callback {
        send_command(EM::DefaultDeferrable.new, 'unsubscribe', [PING_CHANNEL])
      }
    end
  end
end