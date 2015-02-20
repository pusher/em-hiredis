module EventMachine::Hiredis
  module MockConnection

    def send_data(data)
      @expectations ||= []
      expectation = @expectations.shift
      if expectation
        data.to_s.should == expectation[:command]

        handle_response(expectation[:response]) if expectation[:response]
      else
        fail("Unexpected command #{command}, #{args}")
      end
    end

    def marshal(*args)
      args.flatten.join(' ')
    end

    def close_connection
      unbind
    end

    def _expect(command, response = 'OK')
      @expectations ||= []
      @expectations << { command: command, response: response }
    end

    def _expect_no_response(command)
      _expect(command, nil)
    end

    def _expectations_met!
      if @expectations && @expectations.length > 0
        fail("Did not receive expected command #{@expectations.shift}")
      end
    end
  end
end
