module EventMachine::Hiredis
  module MockConnection

    def send_command(df, command, args)
      @response_queue << df

      args = [args].flatten
      @expectations ||= []
      expectation = @expectations.shift
      if expectation
        command.to_s.should == expectation[:command]
        args.should == expectation[:args]

        expectation[:blk].call(df) if expectation[:blk]
      else
        fail("Unexpected command #{command}, #{args}")
      end

      df
    end

    def _expect(command, *args, &blk)
      @expectations ||= []
      blk = lambda { |df| df.succeed } unless blk
      @expectations << { command: command, args: args, blk: blk }
    end

    def _expectations_met!
      if @expectations && @expectations.length > 0
        fail("Did not receive expected command #{@expectations.shift}")
      end
    end

    def _connect
      connection_completed
    end

    def close_connection
      unbind
    end

  end
end
