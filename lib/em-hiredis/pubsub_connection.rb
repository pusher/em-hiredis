module EventMachine::Hiredis
  module PubsubConnection
    include EventMachine::Hiredis::EventEmitter

    PUBSUB_COMMANDS = %w{subscribe unsubscribe psubscribe punsubscribe}.freeze
    PUBSUB_MESSAGES = %w{message pmessage subscribe unsubscribe psubscribe punsubscribe}.freeze

    def initialize
      super
      @reader = ::Hiredis::Reader.new
      @response_queues = Hash.new { |h, k| h[k] = [] }
    end

    def send_command(df, command, channel)
      puts "send #{command} #{channel}"
      raise "Invalid args #{channel}" if channel.size > 1
      channel = channel[0]
      @response_queues[channel] << df
      send_data(marshal(command, channel))
    end

    # EM::Connection callback
    def connection_completed
      emit(:connected)
    end

    # EM::Connection callback
    def receive_data(data)
      @reader.feed(data)
      until (reply = @reader.gets) == false
        puts "reply #{reply}"
        type = reply && reply[0]
        if PUBSUB_COMMANDS.include?(type)
          handle_command_response(*reply)
        end
        emit(reply[0].to_sym, reply[1..-1])
      end
    end

    # EM::Connection callback
    def unbind
      puts "Unbind"
      emit(:disconnected)
    end

    protected

    def handle_command_response(command, channel, sub_count)
      df = @response_queues[channel].pop
      df.succeed(sub_count)

      if @response_queues[channel].empty?
        @response_queues.delete(channel)
      end
    end

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
  end
end