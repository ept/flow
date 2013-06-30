module Flow
  class Transaction
    class << self
      def current
        Thread.current[:flow_transaction]
      end
    end

    def initialize(*roots)
      raise ArgumentError, 'Pass the models you want to modify into the transaction' if roots.empty?
      roots.each do |root|
        raise ArgumentError, "Expected root model, got #{root.class}" unless root.is_a? Flow::Model::RootModel
      end
      @roots = roots
      @model_updates = {}
      @dirty_ancestors = Set.new
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

    def set_latest_model(existing, updated)
      unless @model_updates.include? existing.flow_object_id
        index_item = index_lookup(existing.flow_object_id)
        while index_item && index_item.parent
          @dirty_ancestors << index_item.parent.flow_object_id
          index_item = index_lookup(index_item.parent.flow_object_id)
        end
      end

      @model_updates[existing.flow_object_id] = updated
    end

    def updated(model)
      @model_updates[model.flow_object_id] || model
    end

    def updated_root(model)
      if @dirty_ancestors.include?(model.flow_object_id) || index_lookup(model.flow_object_id)
        updated(model).flow_transaction_update(self)
      else
        updated(model)
      end
    end

    private

    def index_lookup(object_id)
      @roots.each do |root|
        item = root.flow_index[object_id]
        return item if item
      end
      nil
    end
  end

  def self.transaction(*roots, &block)
    tx = Transaction.new(*roots)
    tx.enter
    begin
      yield *roots
    ensure
      tx.exit
    end

    if roots.size == 1
      tx.updated_root(roots.first)
    else
      roots.map {|root| tx.updated_root(root) }
    end
  end
end
