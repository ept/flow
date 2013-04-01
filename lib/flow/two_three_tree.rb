module Flow
  # A 2-3 Tree is a self-balancing search tree data structure. We use it to implement sets, maps,
  # ordered lists (arrays) and more.
  #
  # This implementation is based on Reade's description [1]. It is persistent (in the sense of
  # immutable, not in the sense of writing to disk), i.e. every modification returns a new value,
  # and all references to previous versions remain unchanged. When a modification is made, only a
  # small proportion of the structure is copied, and most of it is shared with previous versions.
  # If you want to know more about persistent data structures, Okasaki's book [2] is good reading.
  #
  # 2-3 Trees have a lot of similarities with Red-Black trees (e.g. see CLRS [3] Chapter 13), but
  # have the advantage of being simpler to implement persistently, with a negligible performance
  # difference.
  #
  # [1] Chris M P Reade: "Balanced trees with removals: an exercise in rewriting and proof", Science
  #     of Computer Programming, vol. 18, no. 2, pp. 181--204, Apr. 1992.
  #     http://www.sciencedirect.com/science/article/pii/016764239290009Z
  # [2] Chris Okasaki: "Purely Functional Data Structures", Cambridge University Press, 1999.
  #     http://www.cambridge.org/gb/knowledge/isbn/item1161740/?site_locale=en_GB
  # [3] Thomas H Cormen, Charles E Leiserson, Ronald L Rivest and Clifford Stein: "Introduction
  #     To Algorithms", Third Edition, MIT Press, July 2009.
  #     http://mitpress.mit.edu/books/introduction-algorithms
  class TwoThreeTree

    # Exceptions of this type are raised if the internal state of the data structure is unexpected.
    # This indicates either a bug, or that something has been messing with our private structures.
    class BadInternalState < StandardError; end

    protected

    def initialize(root=nil)
      @root = root
    end

    private

    # All methods are private; only subclasses should expose public methods.
    #
    # Node objects must respond at least to the following:
    #
    # * Node#type returns 2 if it is a 2-node, or 3 if it is a 3-node.
    # * Node#left returns the left subtree.
    # * Node#middle returns the middle subtree (only on 3-nodes).
    # * Node#right returns the right subtree.
    # * Node#key and Node#value return the first key and value on this node.
    # * Node#key2 and Node#value2 return the second key and value on this node (only on 3-nodes).
    # * Node#compare(key_box) takes the key in a mutable box and returns one of:
    #   :left   if the key points at a location in the left subtree (on 2-nodes or 3-nodes)
    #   :first  if the key points at the first key of this node (on 2-nodes or 3-nodes)
    #   :middle if the key points at a location in the middle subtree (on 3-nodes only)
    #   :second if the key points at the second key of this node (on 3-nodes only)
    #   :right  if the key points at a location in the right subtree (on 2-nodes or 3-nodes)

    def get(key)
      key_box = Box.new(key)
      node = @root
      while node
        case comparison = node.compare(key_box)
        when :left   then node = node.left
        when :middle then node = node.middle
        when :right  then node = node.right
        when :first, :second then return node
        else raise BadInternalState, "Bad #compare return value: #{comparison.inspect}"
        end
      end
      nil
    end


    def balance2(left, key, value, right)
      # "Put" on the left or right
      if left.is_a?(Put)     # tr2(Put(t1, a, t2), b, t3) = Tr3(t1, a, t2, b, t3)
        three(left.left, left.key, left.value, left.right, key, value, right)
      elsif right.is_a?(Put) # tr2(t1, a, Put(t2, b, t3)) = Tr3(t1, a, t2, b, t3)
        three(left, key, value, right.left, right.key, right.value, right.right)

      # "Taken" on the left
      elsif left.is_a?(Taken)
        case right && right.type
        when 2 # tr2(Taken(t1), a, Tr2(t2, b, t3)) = Taken(Tr3(t1, a, t2, b, t3))
          Taken.new(three(left.tree, key, value, right.left, right.key, right.value, right.right))
        when 3 # tr2(Taken(t1), a, Tr3(t2, b, t3, c, t4)) = Tr2(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
          two(two(left.tree, key, value, right.left),
              right.key, right.value,
              two(right.middle, right.key2, right.value2, right.right))
        else raise BadInternalState, 'tr2(Taken _, _, E)'
        end

      # "Taken" on the right
      elsif right.is_a?(Taken)
        case left && left.type
        when 2 # tr2(Tr2(t1, a, t2), b, Taken(t3)) = Taken(Tr3(t1, a, t2, b, t3))
          Taken.new(three(left.left, left.key, left.value, left.right, key, value, right.tree))
        when 3 # tr2(Tr3(t1, a, t2, b, t3), c, Taken(t4)) = Tr2(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
          two(two(left.left, left.key, left.value, left.middle),
              left.key2, left.value2,
              two(left.right, key, value, right.tree))
        else raise BadInternalState, 'tr2(E, _, Taken _)'
        end

      # Neither "Put" nor "Taken"
      else # tr2(t1, a, t2) = Tr2(t1, a, t2)
        two(left, key, value, right)
      end
    end

    def balance3(left, key1, value1, middle, key2, value2, right)
      # One "Put"
      if left.is_a?(Put)      # tr3(Put(t1, a, t2), b, t3, c, t4) = Put(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
        Put.new(two(left.left, left.key, left.value, left.right),
                key1, value1,
                two(middle, key2, value2, right))
      elsif middle.is_a?(Put) # tr3(t1, a, Put(t2, b, t3), c, t4) = Put(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
        Put.new(two(left, key1, value1, middle.left),
                middle.key, middle.value,
                two(middle.right, key2, value2, right))
      elsif right.is_a?(Put)  # tr3(t1, a, t2, b, Put(t3, c, t4)) = Put(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
        Put.new(two(left, key1, value1, middle),
                key2, value2,
                two(right.left, right.key, right.value, right.right))

      # Three "Taken"s
      elsif left.is_a?(Taken) && middle.is_a?(Taken) && right.is_a?(Taken)
        # tr3(Taken(t1), a, Taken(t2), b, Taken(t3)) = Taken(Tr3(t1, a, t2, b, t3))
        Taken.new(three(left.tree, key1, value1, middle.tree, key2, value2, right.tree))

      # Two "Taken"s
      elsif left.is_a?(Taken)   && middle.is_a?(Taken) # tr3(Taken(t1), a, Taken(t2), b, t3) = Tr2(Tr2(t1, a, t2), b, t3)
        two(two(left.tree, key1, value1, middle.tree), key2, value2, right)
      elsif middle.is_a?(Taken) && right.is_a?(Taken)  # tr3(t1, a, Taken(t2), b, Taken(t3)) = Tr2(t1, a, Tr2(t2, b, t3))
        two(left, key1, value1, two(middle.tree, key2, value2, right.tree))
      elsif left.is_a?(Taken)   && right.is_a?(Taken)
        case middle && middle.type
        when 2 # tr3(Taken(t1), a, Tr2(t2, b, t3), c, Taken(t4)) = Tr2(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
          two(two(left.tree, key1, value1, middle.left),
              middle.key, middle.value,
              two(middle.right, key2, value2, right.tree))
        when 3 # tr3(Taken(t1), a, Tr3(t2, b, t3, c, t4), d, Taken(t5)) = Tr2(Tr3(t1, a, t2, b, t3), c, Tr2(t4, d, t5))
          two(three(left.tree, key1, value1, middle.left, middle.key, middle.value, middle.middle),
              middle.key2, middle.value2,
              two(middle.right, key2, value2, right.tree))
        else raise BadInternalState, 'tr3(Taken _, _, E, _, Taken _)'
        end

      # One "Taken" on the left
      elsif left.is_a?(Taken)
        case middle && middle.type
        when 2 # tr3(Taken(t1), a, Tr2(t2, b, t3), c, t4) = Tr2(Tr3(t1, a, t2, b, t3), c, t4)
          two(three(left.tree, key1, value1, middle.left, middle.key, middle.value, middle.right),
              key2, value2,
              right)
        when 3 # tr3(Taken(t1), a, Tr3(t2, b, t3, c, t4), d, t5) = Tr3(Tr2(t1, a, t2), b, Tr2(t3, c, t4), d, t5)
          three(two(left.tree, key1, value1, middle.left),
                middle.key, middle.value,
                two(middle.middle, middle.key2, middle.value2, middle.right),
                key2, value2,
                right)
        else raise BadInternalState, 'tr3(Taken _, _, E, _, _)'
        end

      # One "Taken" on the right
      elsif right.is_a?(Taken)
        case middle && middle.type
        when 2 # tr3(t1, a, Tr2(t2, b, t3), c, Taken(t4)) = Tr2(t1, a, Tr3(t2, b, t3, c, t4))
          two(left,
              key1, value1,
              three(middle.left, middle.key, middle.value, middle.right, key2, value2, right.tree))
        when 3 # tr3(t1, a, Tr3(t2, b, t3, c, t4), d, Taken(t5)) = Tr3(t1, a, Tr2(t2, b, t3), c, Tr2(t4, d, t5))
          three(left,
                key1, value1,
                two(middle.left, middle.key, middle.value, middle.middle),
                middle.key2, middle.value2,
                two(middle.right, key2, value2, right.tree))
        else raise BadInternalState, 'tr3(_, _, E, _, Taken _)'
        end

      # One "Taken" in the middle
      elsif middle.is_a?(Taken)
        case left && left.type
        when 2 # tr3(Tr2(t1, a, t2), b, Taken(t3), c, t4) = Tr2(Tr3(t1, a, t2, b, t3), c, t4)
          two(three(left.left, left.key, left.value, left.right, key1, value1, middle.tree),
              key2, value2,
              right)
        when 3 # tr3(Tr3(t1, a, t2, b, t3), c, Taken(t4), d, t5) = Tr3(Tr2(t1, a, t2), b, Tr2(t3, c, t4), d, t5)
          three(two(left.left, left.key, left.value, left.middle),
                left.key2, left.value2,
                two(left.right, key1, value1, middle.tree),
                key2, value2,
                right)
        else raise BadInternalState, 'tr3(E, _, Taken _, _, _)'
        end

      # Neither "Put" nor "Taken"
      else # tr3(t1, a, t2, b, t3) = Tr3(t1, a, t2, b, t3)
        three(left, key1, value1, middle, key2, value2, right)
      end
    end

    def merge(left, right)
      if left.nil? && right.nil? # merge(E, E) = Taken(E)
        Taken.new(nil)
      elsif left.nil?
        raise BadInternalState, 'merge(E, _)'
      elsif right.nil?
        raise BadInternalState, 'merge(_, E)'
      elsif left.type == 2 && right.type == 2
        # merge(Tr2(t1, a, t2), Tr2(t3, b, t4)) = tr3(Taken(t1), a, merge(t2, t3), b, Taken(t4))
        three(Taken.new(left.left), left.key, left.value,
              merge(left.right, right.left),
              right.key, right.value, Taken.new(right.right))
      elsif left.type == 2 && right.type == 3
        # merge(Tr2(t1, a, t2), Tr3(t3, b, t4, c, t5)) = tr3(Taken(t1), a, merge(t2, t3), b, Tr2(t4, c, t5))
        three(Taken.new(left.left),
              left.key, left.value,
              merge(left.right, right.left),
              right.key, right.value,
              two(right.middle, right.key2, right.value2, right.right))
      elsif left.type == 3 && right.type == 2
        # merge(Tr3(t1, a, t2, b, t3), Tr2(t4, c, t5)) = tr3(Tr2(t1, a, t2), b, merge(t3, t4), c, Taken(t5))
        three(two(left.left, left.key, left.value, left.middle),
              left.key2, left.value2,
              merge(left.right, right.left),
              right.key, right.value,
              Taken.new(right.right))
      elsif left.type == 3 && right.type == 3
        # merge(Tr3(t1, a, t2, b, t3), Tr3(t4, c, t5, d, t6)) = tr3(Tr2(t1, a, t2), b, merge(t3, t4), c, Tr2(t5, d, t6))
        three(two(left.left, left.key, left.value, left.middle),
              left.key2, left.value2,
              merge(left.right, right.left),
              right.key, right.value,
              two(right.middle, right.key2, right.value2, right.right))
      else
        raise BadInternalState, 'merge of inappropriate trees'
      end
    end

    def left_put(left, key, value, right)
      if left.nil?
        raise BadInternalState, 'left_put(E, _, _)'
      elsif left.is_a?(Taken) # leftPut(Taken(t1), a, t2) = Tr2(t1, a, t2)
        two(left.tree, key, value, right)
      elsif left.type == 2 # leftPut(Tr2(t1, a, t2), b, t3) = Tr3(t1, a, t2, b, t3)
        three(left.left, left.key, left.value, left.right, key, value, right)
      elsif left.type == 3 # leftPut(Tr3(t1, a, t2, b, t3), c, t4) = Put(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
        Put.new(two(left.left, left.key, left.value, left.middle),
                left.key2, left.value2,
                two(left.right, key, value, right))
      else
        raise BadInternalState, 'left_put of inappropriate tree'
      end
    end

    def right_put(left, key, value, right)
      if right.nil?
        raise BadInternalState, 'right_put(_, _, E)'
      elsif right.is_a?(Taken) # rightPut(t1, a, Taken(t2)) = Tr2(t1, a, t2)
        two(left, key, value, right.tree)
      elsif right.type == 2 # rightPut(t1, a, Tr2(t2, b, t3)) = Tr3(t1, a, t2, b, t3)
        three(left, key, value, right.left, right.key, right.value, right.right)
      elsif right.type == 3 # rightPut(t1, a, Tr3(t2, b, t3, c, t4)) = Put(Tr2(t1, a, t2), b, Tr2(t3, c, t4))
        Put.new(two(left, key, value, right.left),
                right.key, right.value,
                two(right.middle, right.key2, right.value2, right.right))
      else
        raise BadInternalState, 'right_put of inappropriate tree'
      end
    end

    def add(key, value)
      new_node = add_internal(@root, Box.new(key), value)
      if new_node.equal? @root
        self
      else
        new_node = two(new_node.left, new_node.key, new_node.value, new_node.right) if new_node.is_a? Put
        self.class.new(new_node)
      end
    end

    def add_internal(node, key_box, value)
      case node && node.type
      when nil then Put.new(nil, key_box.value, value, nil)

      when 2
        case comparison = node.compare(key_box)
        when :left
          new_left = add_internal(node.left, key_box, value)
          new_left.equal?(node.left) ? node : balance2(new_left, node.key, node.value, node.right)
        when :right
          new_right = add_internal(node.right, key_box, value)
          new_right.equal?(node.right) ? node : balance2(node.left, node.key, node.value, new_right)
        when :first
          node.value == value ? node : two(node.left, node.key, value, node.right)
        else raise BadInternalState, "Bad #compare return value: #{comparison.inspect}"
        end

      when 3
        case comparison = node.compare(key_box)
        when :left
          new_left = add_internal(node.left, key_box, value)
          if new_left.equal?(node.left)
            node
          else
            balance3(new_left, node.key, node.value, node.middle, node.key2, node.value2, node.right)
          end
        when :middle
          new_middle = add_internal(node.middle, key_box, value)
          if new_middle.equal?(node.middle)
            node
          else
            balance3(node.left, node.key, node.value, new_middle, node.key2, node.value2, node.right)
          end
        when :right
          new_right = add_internal(node.right, key_box, value)
          if new_right.equal?(node.right)
            node
          else
            balance3(node.left, node.key, node.value, node.middle, node.key2, node.value2, new_right)
          end
        when :first
          if node.value == value
            node
          else
            three(node.left, node.key, value, node.middle, node.key2, node.value2, node.right)
          end
        when :second
          if node.value2 == value
            node
          else
            three(node.left, node.key, node.value, node.middle, node.key2, value, node.right)
          end
        else raise BadInternalState, "Bad #compare return value: #{comparison.inspect}"
        end

      else raise BadInternalState, "Bad node type: #{node.type}"
      end
    end

    def remove(key, value_box)
      new_node = remove_internal(@root, Box.new(key), value_box)
      if new_node.equal? @root
        self
      else
        new_node = two(new_node.left, new_node.key, new_node.value, new_node.right) if new_node.is_a? Put
        new_node = new_node.tree if new_node.is_a? Taken
        self.class.new(new_node)
      end
    end

    def remove_internal(node, key_box, value_box)
      case node && node.type
      when nil then nil

      when 2
        case comparison = node.compare(key_box)
        when :left
          new_left = remove_internal(node.left, key_box, value_box)
          new_left.equal?(node.left) ? node : balance2(new_left, node.key, node.value, node.right)
        when :right
          new_right = remove_internal(node.right, key_box, value_box)
          new_right.equal?(node.right) ? node : balance2(node.left, node.key, node.value, new_right)
        when :first
          value_box.value = node.value
          merge(node.left, node.right)
        else raise BadInternalState, "Bad #compare return value: #{comparison.inspect}"
        end

      when 3
        case comparison = node.compare(key_box)
        when :left
          new_left = remove_internal(node.left, key_box, value_box)
          if new_left.equal?(node.left)
            node
          else
            balance3(new_left, node.key, node.value, node.middle, node.key2, node.value2, node.right)
          end
        when :middle
          new_middle = remove_internal(node.middle, key_box, value_box)
          if new_middle.equal?(node.middle)
            node
          else
            balance3(node.left, node.key, node.value, new_middle, node.key2, node.value2, node.right)
          end
        when :right
          new_right = remove_internal(node.right, key_box, value_box)
          if new_right.equal?(node.right)
            node
          else
            balance3(node.left, node.key, node.value, node.middle, node.key2, node.value2, new_right)
          end
        when :first
          value_box.value = node.value
          left_put(merge(node.left, node.middle), node.key2, node.value2, node.right)
        when :second
          value_box.value = node.value2
          right_put(node.left, node.key, node.value, merge(node.middle, node.right))
        else raise BadInternalState, "Bad #compare return value: #{comparison.inspect}"
        end

      else raise BadInternalState, "Bad node type: #{node.type}"
      end
    end

    # Create a new 2-node. Subclasses should override this.
    def two(left, key, value, right)
      raise NotImplementedError
    end

    # Create a new 3-node. Subclasses should override this.
    def three(left, key1, value1, middle, key2, value2, right)
      raise NotImplementedError
    end

    # A mutable object for simulating multiple return values. The same effect could be achieved
    # by returning an array (or some other composite object), but a box uses fewer allocations.
    # @private
    class Box < Struct.new(:value); end

    # A temporary placeholder for a 2-node that doesn't count towards tree depth.
    # @private
    class Put < Struct.new(:left, :key, :value, :right); end

    # A temporary placeholder for one additional level of tree depth.
    # @private
    class Taken < Struct.new(:tree); end

    class Map < TwoThreeTree
      def [](key)
        node = get(key)
        node.key == key ? node.value : node.value2 if node
      end

      def set(key, value)
        add(key, value)
      end

      def delete(key)
        value_box = Box.new(nil)
        [remove(key, value_box), value_box.value]
      end

      private

      def two(left, key, value, right)
        TwoNode.new(left, key, value, right)
      end

      def three(left, key1, value1, middle, key2, value2, right)
        ThreeNode.new(left, key1, value1, middle, key2, value2, right)
      end

      # @private
      class TwoNode
        attr_reader :left, :key, :value, :right

        def initialize(left, key, value, right)
          @left = left; @key = key; @value = value; @right = right
        end

        def type; 2; end

        def compare(key_box, value=nil)
          case comparison = (key_box.value <=> key)
          when -1 then :left
          when  0 then :first
          when +1 then :right
          else raise BadInternalState, "bad <=> return value #{comparison.inspect}"
          end
        end
      end

      # @private
      class ThreeNode
        attr_reader :left, :key, :value, :middle, :key2, :value2, :right

        def initialize(left, key, value, middle, key2, value2, right)
          @left = left; @key = key; @value = value; @middle = middle
          @key2 = key2; @value2 = value2; @right = right
        end

        def type; 3; end

        def compare(key_box, value=nil)
          case comparison1 = (key_box.value <=> key)
          when -1 then :left
          when  0 then :first
          when +1
            case comparision2 = (key_box.value <=> key2)
            when -1 then :middle
            when  0 then :second
            when +1 then :right
            else raise BadInternalState, "bad <=> return value #{comparison2.inspect}"
            end
          else raise BadInternalState, "bad <=> return value #{comparison1.inspect}"
          end
        end
      end
    end
  end
end
