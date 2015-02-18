module EventMachine::Hiredis
  module ReqRespConnection
    include EventMachine::Hiredis::EventEmitter

    def initialize
      super
      @reader = ::Hiredis::Reader.new
      @response_queue = []
    end

    def send_command(df, command, args)
      @response_queue.push(df)
      puts "send #{command} #{args}"
      send_data(marshal(command, *args))
      return df
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
        handle_incoming(reply)
      end
    end

    # EM::Connection callback
    def unbind
      puts "Unbind"
      @response_queue.each { |df| df.fail(EM::Hiredis::Error.new('Redis connection lost')) }
      @response_queue.clear
      emit(:disconnected)
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

    def handle_incoming(reply)
      df = @response_queue.shift
      if df
        if RuntimeError === reply
          e = EM::Hiredis::RedisError.new(reply.message)
          e.redis_error = reply
          df.fail(e)
        else
          df.succeed(reply)
        end
      else
        emit(:replies_out_of_sync)
        close_connection
      end
    end
  end
end
