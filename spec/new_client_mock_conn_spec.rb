require 'spec_helper'
require 'support/inprocess_redis_mock'

describe EM::Hiredis::BaseClient do
  default_timeout 4

  class ClientTestConnection
    include EM::Hiredis::ReqRespConnection
    include EM::Hiredis::MockConnection
  end

  # Create expected_connections connections, inject them in order in to the
  # client as it creates new ones
  def mock_connections(expected_connections)
    connections = []
    expected_connections.times { connections << ClientTestConnection.new }
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

  context 'disconnects from em' do
    it 'should retry when connecting' do
      mock_connections(2) { |client, (conn_a, conn_b)|
        connected = false
        client.connect.callback {
          connected = true
        }.errback {
          fail('Connection failed')
        }

        # not connected yet
        conn_a.unbind

        conn_b._expect('select', 9)
        conn_b._connect

        connected.should == true
      }
    end

    it 'should retry when partially set up' do
      mock_connections(2) { |client, (conn_a, conn_b)|
        conn_a._expect('select', 9) { next } # leave the deferrable hanging

        connected = false
        client.connect.callback {
          connected = true
        }.errback {
          fail('Connection failed')
        }

        conn_a._connect
        # awaiting response to 'select'
        conn_a.unbind

        conn_b._expect('select', 9)
        conn_b._connect

        connected.should == true
      }
    end

    it 'should reconnect once connected' do
      mock_connections(2) { |client, (conn_a, conn_b)|
        # should reconnect immediately from connected state
        client.should_not_receive(:em_timer)

        conn_a._expect('select', 9)

        client.connect.errback {
          fail('Connection failed')
        }

        reconnected = false
        client.on(:reconnected) {
          reconnected = true
        }

        conn_a._connect
        # awaiting response to 'select'
        conn_a.unbind

        conn_b._expect('select', 9)
        conn_b._connect

        reconnected.should == true
      }
    end
  end
end
