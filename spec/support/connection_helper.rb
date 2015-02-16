module ConnectionHelper
  # Use db 9 for tests to avoid flushing the main db
  # It would be nice if there was a standard db number for testing...
  def connect(timeout = 1, uri = 'redis://localhost:6379/9')
    em(timeout) {
      redis = EM::Hiredis.connect(uri).callback {
        redis.flushdb
      }
      yield redis
    }
  end

  def connect_pubsub(timeout = 1, uri = 'redis://localhost:6379/9')
    em(timeout) {
      redis = EM::Hiredis.setup(uri).connect
      pubsub = EM::Hiredis.setup_pubsub(uri).connect
      yield redis, pubsub
    }
  end
end
