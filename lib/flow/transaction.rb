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

    def set_latest_model(updated)
      index_item = index_lookup(updated.flow_object_id)
      raise 'Cannot modify an object that is not part of the transaction object tree' if index_item.nil?

      ancestor = index_item
      while ancestor.parent
        @dirty_ancestors << ancestor.parent.flow_object_id
        ancestor = index_lookup(ancestor.parent.flow_object_id)
        raise 'Cannot modify a tree that is not part of the transaction object tree' if ancestor.nil?
      end

      unless @roots.any? {|root| root.flow_object_id == ancestor.model.flow_object_id }
        raise 'Modified object is not in the current transaction scope'
      end

      @model_updates[updated.flow_object_id] = IndexItem.new(updated, index_item.parent)
    end

    def set_latest_parent(child, parent)
      # Mark old ancestor chain as dirty, if applicable.
      # TODO make sure the child is removed from its old parent.
      set_latest_model(child) if index_lookup(child.flow_object_id)

      @model_updates[child.flow_object_id] = IndexItem.new(child, parent)
      set_latest_model(child) # Mark new ancestor chain as dirty
    end

    def updated(model)
      index_item = @model_updates[model.flow_object_id]
      index_item ? index_item.model : model
    end

    def updated_root(model)
      if @dirty_ancestors.include?(model.flow_object_id)
        updated(model).flow_transaction_update(self)
      else
        updated(model)
      end
    end

    private

    def index_lookup(object_id)
      return @model_updates[object_id] if @model_updates.include?(object_id)
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
