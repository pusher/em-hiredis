require 'set'

module EventMachine::Hiredis
  class StateMachine
    include EventMachine::Hiredis::EventEmitter

    attr_reader :state

    def initialize
      @transitions = {}
      @state = :initial
      @all_states = Set.new([:initial])
    end

    def add_transition(name, from, to)
      existing = @transitions.find { |_, v| v == [from, to] }
      if existing
        raise "Duplicate transition #{from}, #{to}, already exists as #{existing.first}"
      end

      @all_states.add(from)
      @all_states.add(to)
      @transitions[name] = [from, to]
    end

    def update_state(to)
      raise "Invalid state #{to}" unless @all_states.include?(to)

      transition = @transitions.find { |_, v| v == [@state, to] }
      raise "No such transition #{@state} #{to}" unless transition

      old_state = @state
      @state = to
      emit(to, old_state)
    end
  end
end
