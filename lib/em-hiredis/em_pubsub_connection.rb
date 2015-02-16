module EventMachine::Hiredis
  class EMPubsubConnection < EM::Connection
    include EventMachine::Hiredis::PubsubConnection
  end
end
