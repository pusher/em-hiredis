require 'spec_helper'

def connect_mock(activity_timeout, response_timeout)
  em(10) do
    server = NetworkedRedisMock::RedisMock.new
    redis = EventMachine::Hiredis.connect('redis://localhost:6381', activity_timeout, response_timeout)
    yield redis, server
  end
end

describe EM::Hiredis::Client do
  it 'should ping after activity timeout reached' do
    connect_mock(2, 1) do |redis, server|
      EM.add_timer(4) {
        server.received.should include('ping')
        done
      }
    end
  end

  it 'should not ping before activity timeout reached' do
    connect_mock(3, 1) do |redis, server|
      EM.add_timer(2) {
        server.received.should_not include('ping')
        done
      }
    end
  end

  it 'should not ping if there is activity' do
    connect_mock(2, 1) do |redis, server|
      EM.add_timer(2) {
        redis.get('test')
      }

      EM.add_timer(4) {
        server.received.should_not include('ping')
        done
      }
    end
  end

  it 'should ping after timeout reached even though command has been sent (no response)' do
    connect_mock(2, 1) do |redis, server|
      server.pause # no responses from now on

      EM.add_timer(1.5) {
        redis.get('test')
      }

      EM.add_timer(4) {
        server.received.should include('ping')
        done
      }
    end
  end

  it 'should trigger a reconnect when theres no response to ping' do
    connect_mock(2, 1) do |redis, server|
      server.pause # no responses from now on

      EM.add_timer(1.5) {
        redis.get('test')
      }

      EM.add_timer(5) {
        server.received.should include('disconnect')
        done
      }
    end
  end
end
