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

    # Expect a command a respond with specified response
    def _expect(command, response = 'OK')
      @expectations ||= []
      @expectations << { command: command, response: response }
    end

    # Expect a command and do not respond
    def _expect_no_response(command)
      _expect(command, nil)
    end

    # Expect and command and response with same
    # This is the basic form of the redis pubsub protocol's acknowledgements
    def _expect_and_echo(command)
      _expect(command, command.split(' '))
    end

    def _expectations_met!
      if @expectations && @expectations.length > 0
        fail("Did not receive expected command #{@expectations.shift}")
      end
    end
  end

  class MockConnectionEM
    attr_reader :connections

    def initialize(expected_connections, conn_class)
      @timers = Set.new
      @connections = []
      expected_connections.times { @connections << conn_class.new }
      @connection_index = 0
    end

    def connect(host, port, connection_class, *args)
      connection = @connections[@connection_index]
      @connection_index += 1
      connection
    end

    def add_timer(delay, &blk)
      timer = Object.new
      @timers.add(timer)
      blk.call

      return timer
    end

    def cancel_timer(timer)
      marker = @timers.delete(timer)
      marker.should_not == nil
    end
  end
end
