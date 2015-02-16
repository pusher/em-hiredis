module EventMachine::Hiredis
  module PubsubConnection
    include ReqRespConnection
    include EventMachine::Hiredis::EventEmitter

    PUBSUB_COMMANDS = %w{subscribe unsubscribe psubscribe punsubscribe}.freeze
    PUBSUB_MESSAGES = %w{message pmessage}.freeze

    def initialize
      super
      @reader = ::Hiredis::Reader.new
      @response_queues = Hash.new { |h, k| h[k] = [] }
    end

    def send_command(df, command, args)
      if PUBSUB_COMMANDS.include?(command.to_s)
        puts "send #{command} #{args}"
        raise "Invalid args #{args}" if args.size > 1
        channel = args[0]
        @response_queues[channel] << df
        send_data(marshal(command, channel))
      else
        super
      end
    end

    protected

    def handle_incoming(reply)
      type = reply[0]
      if PUBSUB_COMMANDS.include?(type)
        _, channel, sub_count = reply
        df = @response_queues[channel].pop
        df.succeed(sub_count)

        if @response_queues[channel].empty?
          @response_queues.delete(channel)
        end
        emit(type.to_sym, reply[1..-1])
      elsif PUBSUB_MESSAGES.include?(type)
        emit(type.to_sym, reply[1..-1])
      else
        super(reply)
      end
    end
  end
end