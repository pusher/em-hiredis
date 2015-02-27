require 'spec_helper'

# NB much of the connection manager is currently "integration tested" via
# tests on the clients. More unit tests would be beneficial if the behaviour
# evolves at all.

describe EM::Hiredis::ConnectionManager do

  class Box
    def put(callable)
      @callable = callable
    end
    def call
      @callable.call
    end
  end

  class RSpec::Mocks::Mock
    def expect_event_registration(event)
      callback = Box.new
      self.should_receive(:on) do |e, &cb|
        e.should == event
        callback.put(cb)
      end
      return callback
    end
  end

  context 'forcing reconnection' do
    it 'should be successful when disconnected from initial attempt failure' do
      em = EM::Hiredis::TimeMockEventMachine.new
      conn_factory = mock('connection factory')

      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new

      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        second_conn_df
      )

      manager.connect

      initial_conn_df.fail('Testing')
      manager.state.should == :disconnected

      manager.reconnect

      manager.state.should == :connecting

      # Which will succeed
      conn = mock('connection')
      conn.expect_event_registration(:disconnected)
      second_conn_df.succeed(conn)

      manager.state.should == :connected

      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end

    it 'should be successful when disconnected from existing connection, triggered on disconnected' do
      em = EM::Hiredis::TimeMockEventMachine.new
      conn_factory = mock('connection factory')

      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new

      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        second_conn_df
      )

      manager.connect

      initial_conn = mock('initial connection')
      disconnected_callback = initial_conn.expect_event_registration(:disconnected)
      initial_conn_df.succeed(initial_conn)

      manager.state.should == :connected

      manager.on(:disconnected) {
        manager.state.should == :disconnected

        manager.reconnect

        manager.state.should == :connecting

        second_conn = mock('second connection')
        second_conn.expect_event_registration(:disconnected)
        second_conn_df.succeed(second_conn)
      }

      disconnected_callback.call      

      manager.state.should == :connected

      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end

    it 'should be successful when disconnected from existing connection, triggered on reconnect_failed' do
      em = EM::Hiredis::TimeMockEventMachine.new
      conn_factory = mock('connection factory')

      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      fail_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new

      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        fail_conn_df,
        second_conn_df
      )

      manager.connect

      initial_conn = mock('initial connection')
      disconnected_callback = initial_conn.expect_event_registration(:disconnected)
      initial_conn_df.succeed(initial_conn)

      manager.state.should == :connected

      manager.on(:reconnect_failed) {
        manager.state.should == :disconnected

        manager.reconnect

        manager.state.should == :connecting

        second_conn = mock('second connection')
        second_conn.expect_event_registration(:disconnected)
        second_conn_df.succeed(second_conn)
      }

      fail_conn_df.fail('Testing')
      disconnected_callback.call      

      manager.state.should == :connected

      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end

    it 'should be successful when connecting (initial connection)' do
      em = EM::Hiredis::TimeMockEventMachine.new

      conn_factory = mock('connection factory')
      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new
      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        second_conn_df
      )

      manager.connect

      manager.state.should == :connecting

      manager.reconnect
      # What happens here?
    end

    it 'should be successful when connecting (reconnect)' do
      em = EM::Hiredis::TimeMockEventMachine.new

      conn_factory = mock('connection factory')
      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new
      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        second_conn_df
      )

      manager.connect

      # Fail the initial connection
      initial_conn_df.fail('Testing')
      # Which adds a reconnect timer, process it
      em.evaluate_ticks

      manager.state.should == :connecting

      manager.reconnect
      # What happens here?
    end

    it 'should be successful when connected' do
      em = EM::Hiredis::TimeMockEventMachine.new
      conn_factory = mock('connection factory')

      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new
      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        second_conn_df
      )

      manager.connect

      initial_conn = mock('initial connection')
      disconnected_callback = initial_conn.expect_event_registration(:disconnected)
      initial_conn_df.succeed(initial_conn)

      manager.state.should == :connected

      # Calling reconnect will ask the current connection to close
      initial_conn.should_receive(:close_connection)

      manager.reconnect

      # Now the connection has finished closing...
      disconnected_callback.call

      # ...we complete the reconnect
      manager.state.should == :connecting

      second_conn = mock('second connection')
      second_conn.should_receive(:on).with(:disconnected)
      second_conn_df.succeed(second_conn)

      manager.state.should == :connected

      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end

    it 'should be successful when failed during initial connect' do
      em = EM::Hiredis::TimeMockEventMachine.new
      conn_factory = mock('connection factory')

      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      # Five failed attempts takes us to failed, then one successful at the end
      fail_conn_df = EM::DefaultDeferrable.new
      succeed_conn_df = EM::DefaultDeferrable.new

      conn_factory.should_receive(:call).and_return(
        fail_conn_df,
        fail_conn_df,
        fail_conn_df,
        fail_conn_df,
        fail_conn_df,
        succeed_conn_df
      )

      manager.connect

      fail_conn_df.fail('Testing')

      # Go through 4 retries scheduled via timer
      em.evaluate_ticks
      em.evaluate_ticks
      em.evaluate_ticks
      em.evaluate_ticks

      manager.state.should == :failed

      manager.reconnect

      conn = mock('connection')
      conn.expect_event_registration(:disconnected)
      succeed_conn_df.succeed(conn)

      manager.state.should == :connected

      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end

    it 'should be successful when failed after initial success' do
      em = EM::Hiredis::TimeMockEventMachine.new

      conn_factory = mock('connection factory')
      manager = EM::Hiredis::ConnectionManager.new(conn_factory, em)

      # Connect successfully, then five failed attempts takes us to failed,
      # then one successful at the end
      fail_conn_df = EM::DefaultDeferrable.new
      initial_conn_df = EM::DefaultDeferrable.new
      second_conn_df = EM::DefaultDeferrable.new

      conn_factory.should_receive(:call).and_return(
        initial_conn_df,
        fail_conn_df,
        fail_conn_df,
        fail_conn_df,
        fail_conn_df,
        second_conn_df
      )

      manager.connect

      initial_conn = mock('initial connection')
      disconnected_cb = initial_conn.expect_event_registration(:disconnected)
      initial_conn_df.succeed(initial_conn)

      manager.state.should == :connected

      disconnected_cb.call

      fail_conn_df.fail('Testing')

      # Go through 4 retries scheduled via timer
      em.evaluate_ticks
      em.evaluate_ticks
      em.evaluate_ticks
      em.evaluate_ticks

      manager.state.should == :failed

      manager.reconnect

      second_conn = mock('second connection')
      second_conn.expect_event_registration(:disconnected)
      second_conn_df.succeed(second_conn)

      manager.state.should == :connected
    
      # Reconnect timers should have been cancelled
      em.remaining_timers.should == 0
    end
  end
end
