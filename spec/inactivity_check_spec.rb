require 'spec_helper'
require 'support/inprocess_redis_mock'

def connect_mock(timeout = 10, url = 'redis://localhost:6381')
  em(timeout) do
    IRedisMock.start
    redis = EventMachine::Hiredis.connect(url)
    yield redis, IRedisMock
  end
end

describe EM::Hiredis::Client do
  it 'should ping after activity timeout reached' do
    connect_mock do |redis, server|
      redis.configure_inactivity_check(2, 1)
      EM.add_timer(4) {
        server.received.should include('ping')
        done
      }
    end
  end

  it 'should not ping before activity timeout reached' do
    connect_mock do |redis, server|
      redis.configure_inactivity_check(3, 1)
      EM.add_timer(2) {
        server.received.should_not include('ping')
        done
      }
    end
  end

  it 'should not ping if there is activity' do
    connect_mock do |redis, server|
      redis.configure_inactivity_check(2, 1)

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
    connect_mock do |redis, server|
      redis.configure_inactivity_check(2, 1)
      IRedisMock.pause # no responses from now on

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
    connect_mock do |redis, server|
      redis.configure_inactivity_check(2, 1)
      IRedisMock.pause # no responses from now on

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
