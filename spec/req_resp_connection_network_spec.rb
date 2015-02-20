require 'spec_helper'
require 'support/inprocess_redis_mock'

def recording_server(replies = {})
  em {
    IRedisMock.start(replies)
    yield IRedisMock
  }
end

describe EM::Hiredis::BaseClient do
  context 'inactivity checks' do
    default_timeout 5

    it 'should fire after an initial period of inactivity' do
      em {
        con = TestReqRespConnection.new(1, 1)
        con.connection_completed

        EM.add_timer(3) {
          con.sent.should include("*1\r\n$4\r\nping\r\n")
          done
        }
      }
    end

    it 'should fire after a later period of inactivity' do
      em {
        con = TestReqRespConnection.new(1, 1)
        con.connection_completed

        EM.add_timer(1.5) {
          con.send_command(EM::DefaultDeferrable.new, 'get', ['x'])
        }

        EM.add_timer(3) {
          con.sent.should_not include("*1\r\n$4\r\nping\r\n")
          done
        }

        EM.add_timer(4) {
          con.sent.should include("*1\r\n$4\r\nping\r\n")
          done
        }
      }
    end
  end
end