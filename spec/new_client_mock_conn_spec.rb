require 'spec_helper'
require 'support/inprocess_redis_mock'

describe EM::Hiredis::NewClient do
  default_timeout 4

  class NonEMReqRespConnection
    include EM::Hiredis::ReqRespConnection

    def send_command(df, command, args)
      @expectations ||= []
      expectation = @expectations.shift
      if expectation
        command.to_s.should == expectation[:command]
        args.should == expectation[:args]

        expectation[:blk].call(df)
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
      if @expectations.length > 0
        fail("Did not receive expected command #{@expectations.shift}")
      end
    end

    def _connect
      connection_completed
    end
  end

  # Create expected_connections connections, inject them in order in to the
  # client as it  
  def mock_connections(expected_connections)
    connections = []
    expected_connections.times { connections << NonEMReqRespConnection.new }
    connection_index = 0

    klass = Class.new(EM::Hiredis::NewClient)
    klass.send(:define_method, :em_connect) {
      connection = connections[connection_index]
      connection_index += 1
      connection
    }

    yield klass.new, connections

    connections.each { |c| c._expectations_met! }
  end

  it 'should queue commands issued while reconnecting' do
    mock_connections(2) { |client, (conn_a, conn_b)|
      # Both connections expect to receive 'select' first
      # But pings 3 and 4 and issued between conn_a being disconnected
      # and conn_b completing its connection
      conn_a._expect('select', 0) { |df| df.succeed }
      conn_a._expect('ping', 1) { |df| df.succeed }
      conn_a._expect('ping', 2) { |df| df.succeed }

      conn_b._expect('select', 0) { |df| df.succeed }
      conn_b._expect('ping', 3) { |df| df.succeed }
      conn_b._expect('ping', 4) { |df| df.succeed }

      client.connect
      conn_a._connect

      client.ping(1)
      client.ping(2)

      conn_a.unbind

      client.ping(3)
      client.ping(4)

      conn_b._connect
    }
  end
end
