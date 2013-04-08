module Flow
  class Transaction
    class << self
      def current
        Thread.current[:flow_transaction]
      end
    end

    def enter
      raise 'already in a transaction' if Transaction.current
      Thread.current[:flow_transaction] = self
    end

    def exit
      raise 'current transaction does not match' unless self.equal?(Transaction.current)
      Thread.current[:flow_transaction] = nil
    end

    def updated_roots
      @updated_roots ||= []
    end

    def add_updated_root(old_root, new_root)
      predecessor = updated_roots.detect{|old, new| new.equal? old_root }
      if predecessor
        predecessor[1] = new_root
      else
        updated_roots << [old_root, new_root]
      end
    end

    def latest_root_version(old_root)
      # TODO use a hash map with object_id as key?
      update = updated_roots.detect{|old, new| old.equal? old_root }
      update ? update[1] : old_root
    end
  end

  def self.transaction(&block)
    t = Transaction.new.tap(&:enter)
    yield
    updated_roots
  ensure
    t.exit
  end
end
