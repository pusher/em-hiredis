require 'spec_helper'
require 'support/inprocess_redis_mock'

def recording_server(replies = {})
  em {
    IRedisMock.start(replies)
    yield IRedisMock
    #IRedisMock.stop
  }
end

describe EM::Hiredis::NewClient do
  context 'initial connections' do
    default_timeout 4

    it 'should not connect on construction' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        server.connection_count.should == 0
        done
      }
    end

    it 'should be connected when connect is called' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.callback {
          server.connection_count.should == 1
          done
        }.errback { |e|
          fail(e)
        }
      }
    end

    it 'should issue select command before succeeding connection' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.callback {
          server.connection_count.should == 1
          server.received[0].should == 'select 0'
          done
        }.errback { |e|
          fail(e)
        }
      }
    end

    it 'should issue select command before emitting :connected' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.on(:connected) {
          server.connection_count.should == 1
          server.received[0].should == 'select 0'
          done
        }
        client.connect
      }
    end
  end

  context 'reconnection' do
    default_timeout 4

    it 'should create a new connection if the existing one reports it has failed' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.callback {
          server.kill_connections
        }
        EM.add_timer(0.1) {
          server.connection_count.should == 2
          done
        }
      }
    end

    it 'should emit both connected and reconnected' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.callback {
          server.kill_connections

          callbacks = []
          client.on(:connected) {
            callbacks.push(:connected)
            if callbacks.sort == [:connected, :reconnected]
              done
            end
          }
          client.on(:reconnected) {
            callbacks.push(:reconnected)
            if callbacks.sort == [:connected, :reconnected]
              done
            end
          }
        }
      }
    end

    context 'failing from initial connect attempt' do
      default_timeout 4

      it 'should make 4 attempts, emitting :reconnect_failed with a count' do
        em {
          client = EM::Hiredis::NewClient.new('localhost', 9999) # assumes nothing listening on 9999

          expected = 1
          client.on(:reconnect_failed) { |count|
            count.should == expected
            expected += 1
            done if count == 4
          }

          client.connect
        }
      end

      it 'after 4 unsuccessful attempts should emit :failed' do
        em {
          client = EM::Hiredis::NewClient.new('localhost', 9999) # assumes nothing listening on 9999

          reconnect_count = 0
          client.on(:reconnect_failed) { |count|
            reconnect_count += 1
          }
          client.on(:failed) {
            reconnect_count.should == 4
            done
          }

          client.connect
        }
      end
    end

    context 'failing after initially being connected' do
      default_timeout 4

      it 'should make 4 attempts, emitting :reconnect_failed with a count' do
        recording_server { |server|
          client = EM::Hiredis::NewClient.new('localhost', 6381)
          client.connect.callback {
            server.stop
            server.kill_connections

            expected = 1
            client.on(:reconnect_failed) { |count|
              count.should == expected
              expected += 1
              done if count == 4
            }
          }
        }
      end

      it 'after 4 unsuccessful attempts should emit :failed' do
        recording_server { |server|
          client = EM::Hiredis::NewClient.new('localhost', 6381)
          client.connect.callback {
            server.stop
            server.kill_connections

            reconnect_count = 0
            client.on(:reconnect_failed) { |count|
              reconnect_count += 1
            }
            client.on(:failed) {
              reconnect_count.should == 4
              done
            }
          }
        }
      end

      it 'should fail commands immediately when in a failed state' do
        recording_server { |server|
          client = EM::Hiredis::NewClient.new('localhost', 6381)
          client.connect.callback {
            server.stop
            server.kill_connections

            client.on(:failed) {
              client.get('foo').errback { |e|
                e.message.should == 'Connection in failed state'
                done
              }
            }
          }
        }
      end
    end
  end

  context 'commands' do
    default_timeout 4

    it 'should be able to send commands' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.callback {
          client.set('test', 'value').callback {
            done
          }
        }
      }
    end

    it 'should queue commands called before connect is called' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.set('test', 'value').callback {
          client.ping.callback {
            done
          }
        }

        client.connect
      }
    end
  end

  context 'db selection' do
    default_timeout 4

    it 'should execute db selection first' do
      recording_server { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.set('test', 'value').callback {
          client.ping.callback {
            server.received.should == [
              'select 0',
              'set test value',
              'ping']
            done
          }
        }

        client.connect
      }
    end

    it 'should class db selection failure as a connection failure' do
      recording_server('select 0' => '-ERR no such db') { |server|
        client = EM::Hiredis::NewClient.new('localhost', 6381)
        client.connect.errback { |e|
          done
        }
      }
    end
  end


end
