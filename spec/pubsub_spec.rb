require 'spec_helper'

describe EventMachine::Hiredis::PubsubClient, '(un)subscribe' do
  describe "subscribing" do
    it "should return deferrable which succeeds with subscribe call result" do
      connect_pubsub do |redis, pubsub|
        df = pubsub.subscribe("channel") { }
        df.should be_kind_of(EventMachine::DefaultDeferrable)
        df.callback { |subscription_count|
          # Subscribe response from redis - indicates that subscription has
          # succeeded and that the current connection has a single
          # subscription
          subscription_count.should == 1
          done
        }
      end
    end

    it "should run the passed block when message received" do
      connect_pubsub do |redis, pubsub|
        pubsub.subscribe("channel") { |message|
          message.should == 'hello'
          done
        }.callback {
          redis.publish('channel', 'hello')
        }
      end
    end

    it "should run the passed proc when message received on channel" do
      connect_pubsub do |redis, pubsub|
        proc = Proc.new { |message|
          message.should == 'hello'
          done
        }
        pubsub.subscribe("channel", proc).callback {
          redis.publish('channel', 'hello')
        }
      end
    end
  end

  describe "unsubscribing" do
    it "should allow unsubscribing a single callback without unsubscribing from redis" do
      connect_pubsub do |redis, pubsub|
        proc1 = Proc.new { |message| fail }
        proc2 = Proc.new { |message|
          message.should == 'hello'
          done
        }
        pubsub.subscribe("channel", proc1).callback {
          pubsub.subscribe("channel", proc2).callback {
            pubsub.unsubscribe_proc("channel", proc1)
            redis.publish("channel", "hello")
          }
        }
      end
    end

    it "should unsubscribe from redis on last proc unsubscription" do
      connect_pubsub do |redis, pubsub|
        proc = Proc.new { |message| }
        pubsub.subscribe("channel", proc).callback { |subs_count|
          subs_count.should == 1
          pubsub.unsubscribe_proc("channel", proc).callback {
            # Slightly awkward way to check that unsubscribe happened:
            pubsub.subscribe('channel2').callback { |count|
              # If count is 1 this implies that channel unsubscribed
              count.should == 1
              done
            }
          }
        }
      end
    end

    it "should allow unsubscribing from redis channel, including all callbacks, and return deferrable for redis unsubscribe" do
      connect_pubsub do |redis, pubsub|
        # Raw pubsub event
        pubsub.on('message') { |channel, message| fail }
        # Block subscription
        df_block = pubsub.subscribe("channel") { |m| fail } # block
        # Proc example
        df_proc = pubsub.subscribe("channel", Proc.new { |m| fail })

        df_block.callback {
          df_proc.callback {
            pubsub.unsubscribe("channel").callback { |remaining_subs|
              remaining_subs.should == 0
              redis.publish("channel", "hello") {
                done
              }
            }
          }
        }
      end
    end
  end

  it "should expose raw pubsub events from redis" do
    channel = "channel"
    callback_count = 0
    connect_pubsub do |redis, pubsub|
      pubsub.on(:subscribe) { |channel, subscription_count|
        # 2. Get subscribe callback
        callback_count += 1
        channel.should == channel
        subscription_count.should == 1

        # 3. Publish on channel
        redis.publish(channel, 'foo')
      }

      pubsub.on(:message) { |channel, message|
        # 4. Get message callback
        callback_count += 1
        channel.should == channel
        message.should == 'foo'

        callback_count.should == 2
        done
      }

      # 1. Subscribe to channel
      pubsub.subscribe(channel)
    end
  end

  it "should resubscribe to all channels on reconnect" do
    callback_count = 0
    connect_pubsub do |redis, pubsub|
      # 1. Subscribe to channels
      pubsub.subscribe('channel1') {
        callback_count += 1
      }
      pubsub.subscribe('channel2') {
        callback_count += 1
        EM.next_tick {
          # 4. Success if both messages have been received
          callback_count.should == 2
          done
        }
      }.callback { |subscription_count|
        subscription_count.should == 2
        # 2. Subscriptions complete. Now force disconnect
        pubsub.instance_variable_get(:@connection).close_connection

        EM.add_timer(0.1) {
          # 3. After giving time to reconnect publish to both channels
          redis.publish('channel1', 'foo')
          redis.publish('channel2', 'bar')
        }

      }

    end
  end
end

describe EventMachine::Hiredis::PubsubClient, 'p(un)subscribe' do
  describe "psubscribing" do
    it "should return deferrable which succeeds with psubscribe call result" do
      connect_pubsub do |redis, pubsub|
        df = pubsub.psubscribe("channel") { }
        df.should be_kind_of(EventMachine::DefaultDeferrable)
        df.callback { |subscription_count|
          # Subscribe response from redis - indicates that subscription has
          # succeeded and that the current connection has a single
          # subscription
          subscription_count.should == 1
          done
        }
      end
    end

    it "should run the passed block when message received" do
      connect_pubsub do |redis, pubsub|
        pubsub.psubscribe("channel:*") { |channel, message|
          channel.should == 'channel:foo'
          message.should == 'hello'
          done
        }.callback {
          redis.publish('channel:foo', 'hello')
        }
      end
    end

    it "should run the passed proc when message received on channel" do
      connect_pubsub do |redis, pubsub|
        proc = Proc.new { |channel, message|
          channel.should == 'channel:foo'
          message.should == 'hello'
          done
        }
        pubsub.psubscribe("channel:*", proc).callback {
          redis.publish('channel:foo', 'hello')
        }
      end
    end
  end

  describe "punsubscribing" do
    it "should allow punsubscribing a single callback without punsubscribing from redis" do
      connect_pubsub do |redis, pubsub|
        proc1 = Proc.new { |channel, message| fail }
        proc2 = Proc.new { |channel, message|
          channel.should == 'channel:foo'
          message.should == 'hello'
          done
        }
        pubsub.psubscribe("channel:*", proc1).callback {
          pubsub.psubscribe("channel:*", proc2).callback {
            pubsub.punsubscribe_proc("channel:*", proc1)
            redis.publish("channel:foo", "hello")
          }
        }
      end
    end

    it "should punsubscribe from redis on last proc punsubscription" do
      connect_pubsub do |redis, pubsub|
        proc = Proc.new { |message| }
        pubsub.psubscribe("channel:*", proc).callback { |subs_count|
          subs_count.should == 1
          pubsub.punsubscribe_proc("channel:*", proc).callback {
            # Slightly awkward way to check that unsubscribe happened:
            pubsub.psubscribe('channel2').callback { |count|
              # If count is 1 this implies that channel unsubscribed
              count.should == 1
              done
            }
          }
        }
      end
    end

    it "should allow punsubscribing from redis channel, including all callbacks, and return deferrable for redis punsubscribe" do
      connect_pubsub do |redis, pubsub|
        # Raw pubsub event
        pubsub.on('pmessage') { |pattern, channel, message| fail }
        # Block subscription
        df_block = pubsub.psubscribe("channel") { |c, m| fail } # block
        # Proc example
        df_proc = pubsub.psubscribe("channel", Proc.new { |c, m| fail })

        df_block.callback {
          df_proc.callback {
            pubsub.punsubscribe("channel").callback { |remaining_subs|
              remaining_subs.should == 0
              redis.publish("channel", "hello") {
                done
              }
            }
          }
        }
      end
    end
  end

  it "should expose raw pattern pubsub events from redis" do
    callback_count = 0
    connect_pubsub do |redis, pubsub|
      pubsub.on(:psubscribe) { |pattern, subscription_count|
        # 2. Get subscribe callback
        callback_count += 1
        pattern.should == "channel:*"
        subscription_count.should == 1

        # 3. Publish on channel
        redis.publish('channel:foo', 'foo')
      }

      pubsub.on(:pmessage) { |pattern, channel, message|
        # 4. Get message callback
        callback_count += 1
        pattern.should == 'channel:*'
        channel.should == 'channel:foo'
        message.should == 'foo'

        callback_count.should == 2
        done
      }

      # 1. Subscribe to channel
      pubsub.psubscribe('channel:*')
    end
  end

  it "should resubscribe to all pattern subscriptions on reconnect" do
    callback_count = 0
    connect_pubsub do |redis, pubsub|
      # 1. Subscribe to channels
      pubsub.psubscribe('foo:*') { |channel, message|
        channel.should == 'foo:a'
        message.should == 'hello foo'
        callback_count += 1
      }
      pubsub.psubscribe('bar:*') { |channel, message|
        channel.should == 'bar:b'
        message.should == 'hello bar'
        callback_count += 1
        EM.next_tick {
          # 4. Success if both messages have been received
          callback_count.should == 2
          done
        }
      }.callback { |subscription_count|
        subscription_count.should == 2
        # 2. Subscriptions complete. Now force disconnect
        pubsub.instance_variable_get(:@connection).close_connection

        EM.add_timer(0.1) {
          # 3. After giving time to reconnect publish to both channels
          redis.publish('foo:a', 'hello foo')
          redis.publish('bar:b', 'hello bar')
        }
      }
    end
  end
end
