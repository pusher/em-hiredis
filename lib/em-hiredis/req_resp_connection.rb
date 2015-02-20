module EventMachine::Hiredis
  module ReqRespConnection
    include EventMachine::Hiredis::EventEmitter

    def initialize(inactivity_trigger_secs = nil, inactivity_response_timeout = 2)
      @reader = ::Hiredis::Reader.new
      @response_queue = []

      @connected = false

      @inactivity_trigger_secs = inactivity_trigger_secs
      @inactivity_response_timeout = inactivity_response_timeout
      @inactivity_check_timer = nil
      @inactive_seconds = 0
    end

    def send_command(df, command, args)
      @response_queue.push(df)
      puts "send #{command} #{args}"
      send_data(marshal(command, *args))
      return df
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

    # EM::Connection callback
    def unbind
      puts "Unbind"
      cancel_inactivity_checks

      @response_queue.each { |df| df.fail(EM::Hiredis::Error.new('Redis connection lost')) }
      @response_queue.clear

      if @connected
        emit(:disconnected)
      else
        emit(:connection_failed)
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

    def schedule_inactivity_checks
      @inactive_seconds = 0
      @inactivity_timer = EM.add_periodic_timer(1) {
        @inactive_seconds += 1
        puts "Checking: #{@inactive_seconds}"
        if @inactive_seconds > @inactivity_trigger_secs + @inactivity_response_timeout
          EM::Hiredis.logger.error "#{@connection} No response to ping, triggering reconnect"
          close_connection
        elsif @inactive_seconds > @inactivity_trigger_secs
          EM::Hiredis.logger.debug "#{@connection} Connection inactive, triggering ping"
          ping
        end
      }
    end

    def cancel_inactivity_checks
      EM.cancel_timer(@inactivity_timer) if @inactivity_timer
    end

    def ping
      send_command(EM::DefaultDeferrable.new, 'ping', [])
    end
  end
end
