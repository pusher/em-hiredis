require 'spec_helper'

describe EM::Hiredis::ReqRespConnection do

  class NonEMReqRespConnection
    include EM::Hiredis::ReqRespConnection

    attr_accessor :sent, :closed

    def send_data(data)
      @sent ||= []
      @sent << data
    end

    def close_connection
      @closed = true
    end
  end

  it 'should marshall command to send' do
    con = NonEMReqRespConnection.new
    con.send_command(EM::DefaultDeferrable.new, 'set', ['x', 'true'])
    con.sent[0].should == "*3\r\n$3\r\nset\r\n$1\r\nx\r\n$4\r\ntrue\r\n"
  end

  it 'should succeed deferrable when response arrives' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    df = mock
    con.send_command(df, 'set', ['x', 'true'])

    df.should_receive(:succeed)

    con.receive_data("+OK\r\n")
  end

  it 'should succeed deferrables in order responses arrive' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    df_a = mock
    df_b = mock
    con.send_command(df_a, 'set', ['x', 'true'])
    con.send_command(df_b, 'set', ['x', 'true'])

    responses = []
    df_a.should_receive(:succeed) {
      responses << :a
    }
    df_b.should_receive(:succeed) {
      responses << :b
      responses.should == [:a, :b]
    }

    con.receive_data("+OK\r\n")
    con.receive_data("+OK\r\n")
  end

  it 'should pass response args to succeeded deferrable' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    df = mock
    con.send_command(df, 'get', ['x'])

    df.should_receive(:succeed).with('true')

    con.receive_data("$4\r\ntrue\r\n")
  end

  it 'should fail deferrable on error response' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    df = mock
    con.send_command(df, 'bar', ['x'])

    df.should_receive(:fail) { |e|
      e.class.should == EM::Hiredis::RedisError
      e.message.should == 'ERR bad command'
    }

    con.receive_data("-ERR bad command\r\n")
  end

  it 'should close the connection if replies are out of sync' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    df = mock
    con.send_command(df, 'get', ['x'])

    df.should_receive(:succeed).with('true')

    con.receive_data("$4\r\ntrue\r\n")
    con.receive_data("$19\r\ndidn't ask for this\r\n")

    con.closed.should == true
  end

  it 'should emit :disconnected when the connection closes' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    emitted = false
    con.on(:disconnected) {
      emitted = true
    }

    con.unbind
    emitted.should == true
  end

  it 'fail all pending responses when the connection closes' do
    con = NonEMReqRespConnection.new
    con.connection_completed

    dfs = [mock, mock, mock]
    dfs.each do |df|
      con.send_command(df, 'get', ['x'])
      df.should_receive(:fail) { |e|
        e.class.should == EM::Hiredis::Error
        e.message.should == 'Redis connection lost'
      }
    end

    con.unbind
  end
end
