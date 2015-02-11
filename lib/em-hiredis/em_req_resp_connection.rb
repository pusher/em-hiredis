module EventMachine::Hiredis
  class EMReqRespConnection < EM::Connection
    include EventMachine::Hiredis::ReqRespConnection
  end
end