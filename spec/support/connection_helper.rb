module ConnectionHelper
  # Use db 9 for tests to avoid flushing the main db
  # It would be nice if there was a standard db number for testing...
  def connect(timeout = 1, uri = nil)
    em(timeout) {
      redis = EM::Hiredis.connect(uri)
      yield redis.connect
    }
  end
end
