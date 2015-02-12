require 'spec_helper'
require 'support/inprocess_redis_mock'

describe EM::Hiredis::BaseClient do
  default_timeout 4

  class NonEMReqRespConnection
    include EM::Hiredis::ReqRespConnection

    def send_command(df, command, args)
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
  end

  # Create expected_connections connections, inject them in order in to the
  # client as it  
  def mock_connections(expected_connections)
    connections = []
    expected_connections.times { connections << NonEMReqRespConnection.new }
    connection_index = 0

    klass = Class.new(EM::Hiredis::BaseClient)
    klass.send(:define_method, :em_connect) {
      connection = connections[connection_index]
      connection_index += 1
      connection
    }

    klass.send(:define_method, :em_timer) { |delay, &blk|
      blk.call
    }

    yield klass.new('redis://localhost:6379/9'), connections

    connections.each { |c| c._expectations_met! }
  end

  it 'should queue commands issued while reconnecting' do
    mock_connections(2) { |client, (conn_a, conn_b)|
      # Both connections expect to receive 'select' first
      # But pings 3 and 4 and issued between conn_a being disconnected
      # and conn_b completing its connection
      conn_a._expect('select', 9) { |df| df.succeed }
      conn_a._expect('ping', 1) { |df| df.succeed }
      conn_a._expect('ping', 2) { |df| df.succeed }

      conn_b._expect('select', 9) { |df| df.succeed }
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

  context 'failed state' do
    default_timeout 2

    it 'should be possible to recover' do
      mock_connections(6) { |client, connections|
        failing_connections = connections[0..4]
        good_connection = connections[5]

        # Connect and fail 5 times
        client.connect
        failing_connections.each { |c| c.unbind }

        # We sohuld now be in the failed state
        got_errback = false
        client.ping.errback { |e|
          e.message.should == 'Redis connection in failed state'
          got_errback = true
        }

        good_connection._expect('select', 9) { |df| df.succeed }
        good_connection._expect('ping') { |df| df.succeed }

        # But after calling connect and completing the connection, we are functional again
        client.connect
        good_connection._connect

        got_callback = false
        client.ping.callback {
          got_callback = true
        }

        got_errback.should == true
        got_callback.should == true
      }
    end

    it 'should queue commands once attempting to recover' do
      mock_connections(6) { |client, connections|
        failing_connections = connections[0..4]
        good_connection = connections[5]

        # Connect and fail 5 times
        client.connect
        failing_connections.each { |c| c.unbind }

        # We sohuld now be in the failed state
        got_errback = false
        client.ping.errback { |e|
          e.message.should == 'Redis connection in failed state'
          got_errback = true
        }

        good_connection._expect('select', 9) { |df| df.succeed }
        good_connection._expect('ping') { |df| df.succeed }

        # But after calling connect, we queue commands even though the connection
        # is not yet complete
        client.connect

        got_callback = false
        client.ping.callback {
          got_callback = true
        }

        good_connection._connect

        got_errback.should == true
        got_callback.should == true
      }
    end
  end
end
