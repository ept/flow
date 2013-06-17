module Flow
  class Transaction
    class << self
      def current
        Thread.current[:flow_transaction]
      end
    end

    def enter
      raise 'already in a transaction' if Transaction.current
      raise 'already entered this transaction' if @entered
      @entered = true
      Thread.current[:flow_transaction] = self
    end

    def exit
      raise 'current transaction does not match' unless self.equal?(Transaction.current)
      raise 'not yet entered this transaction' unless @entered
      raise 'already existed this transaction' if @exited
      @exited = true
      Thread.current[:flow_transaction] = nil
    end

    def updated(model)
      raise 'call #updated when the transaction is complete' unless @exited
      accessor = get_accessor(model)
      accessor ? accessor.updated_model : model
    end

    def accessors
      @accessors ||= []
    end

    def add_accessor(model, accessor)
      raise 'model has duplicate accessors' if get_accessor(model)
      accessors << [model, accessor]
    end

    def get_accessor(model)
      accessor = accessors.detect{|old, new| old.equal? model }
      accessor[1] if accessor
    end
  end

  def self.transaction(&block)
    tx = Transaction.new
    tx.enter
    begin
      yield
    ensure
      tx.exit
    end
    tx
  end
end
