require 'spec_helper'

class Flow::TwoThreeTree
  attr_reader :root
end

describe Flow::TwoThreeTree do

  describe '::Map' do
    def two(left, key, value, right)
      Flow::TwoThreeTree::TwoNode.new(left, key, value, right)
    end

    def three(left, key1, value1, middle, key2, value2, right)
      Flow::TwoThreeTree::ThreeNode.new(left, key1, value1, middle, key2, value2, right)
    end

    def val(key, value)
      Flow::TwoThreeTree::TwoNode.new(nil, key, value, nil)
    end

    def val2(key1, value1, key2, value2)
      Flow::TwoThreeTree::ThreeNode.new(nil, key1, value1, nil, key2, value2, nil)
    end

    describe 'insertion' do
      it 'should represent one element as a 2-node' do
        tree = Flow::TwoThreeTree::Map.new
        tree.root.should be_nil
        tree.set('a', 1).root.should == val('a', 1)
      end

      it 'should apply updates to a single element' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1)
        tree.root.should == val('a', 1)
        tree.set('a', 42).root.should == val('a', 42)
      end

      it 'should add an earlier key to an existing 2-node' do
        tree = Flow::TwoThreeTree::Map.new.set('b', 2)
        tree.root.should == val('b', 2)
        tree.set('a', 1).root.should == val2('a', 1, 'b', 2)
      end

      it 'should add a later key to an existing 2-node' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1)
        tree.root.should == val('a', 1)
        tree.set('b', 2).root.should == val2('a', 1, 'b', 2)
      end

      it 'should split when inserting before a 3-node' do
        tree = Flow::TwoThreeTree::Map.new.set('b', 2).set('c', 3)
        tree.root.should == val2('b', 2, 'c', 3)
        tree.set('a', 1).root.should == two(val('a', 1), 'b', 2, val('c', 3))
      end

      it 'should split when inserting in the middle of a 3-node' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1).set('c', 3)
        tree.root.should == val2('a', 1, 'c', 3)
        tree.set('b', 2).root.should == two(val('a', 1), 'b', 2, val('c', 3))
      end

      it 'should split when inserting after a 3-node' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1).set('b', 2)
        tree.root.should == val2('a', 1, 'b', 2)
        tree.set('c', 3).root.should == two(val('a', 1), 'b', 2, val('c', 3))
      end

      it 'should update a value in a 3-node' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1).set('b', 2)
        tree.root.should == val2('a', 1, 'b', 2)
        tree.set('b', 9).root.should == val2('a', 1, 'b', 9)
      end

      it 'should propagate splits up the search path' do
        tree = Flow::TwoThreeTree::Map.new.set('a', 1).set('b', 2).set('c', 3).set('d', 4).set('e', 5).set('f', 6)
        tree.set('g', 7).root.should == two(two(val('a', 1), 'b', 2, val('c', 3)), 'd', 4, two(val('e', 5), 'f', 6, val('g', 7)))
        tree.root.should == three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val2('e', 5, 'f', 6))
      end
    end

    describe 'deletion' do
      it 'should ignore deletion of a nonexistent key' do
        tree, value = Flow::TwoThreeTree::Map.new.set('a', 1).delete('b')
        tree.root.should == val('a', 1)
        value.should be_nil
      end

      it 'should delete a singleton element' do
        tree, value = Flow::TwoThreeTree::Map.new.set('a', 1).delete('a')
        tree.root.should be_nil
        value.should == 1
      end

      it 'should delete the first key of a 3-node' do
        tree, value = Flow::TwoThreeTree::Map.new.set('a', 1).set('b', 2).delete('a')
        tree.root.should == val('b', 2)
        value.should == 1
      end

      it 'should delete the second key of a 3-node' do
        tree, value = Flow::TwoThreeTree::Map.new.set('a', 1).set('b', 2).delete('b')
        tree.root.should == val('a', 1)
        value.should == 2
      end

      it 'should delete the parent of two 2-nodes' do
        tree, value = Flow::TwoThreeTree::Map.new(two(val('a', 1), 'b', 2, val('c', 3))).delete('b')
        tree.root.should == val2('a', 1, 'c', 3)
        value.should == 2
      end

      it 'should delete the parent of a 2-node and a 3-node' do
        tree, value = Flow::TwoThreeTree::Map.new(two(val('a', 1), 'b', 2, val2('c', 3, 'd', 4))).delete('b')
        tree.root.should == two(val('a', 1), 'c', 3, val('d', 4))
        value.should == 2
      end

      it 'should delete the parent of a 3-node and a 2-node' do
        tree, value = Flow::TwoThreeTree::Map.new(two(val2('a', 1, 'b', 2), 'c', 3, val('d', 4))).delete('c')
        tree.root.should == two(val('a', 1), 'b', 2, val('d', 4))
        value.should == 3
      end

      it 'should delete the parent of two 3-nodes' do
        tree, value = Flow::TwoThreeTree::Map.new(two(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5))).delete('c')
        tree.root.should == two(val2('a', 1, 'b', 2), 'd', 4, val('e', 5))
        value.should == 3
      end

      it 'should delete the first key in a parent of 2, 2 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val('e', 5))
        ).delete('b')
        tree.root.should == two(val2('a', 1, 'c', 3), 'd', 4, val('e', 5))
        value.should == 2
      end

      it 'should delete the second key in a parent of 2, 2 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val('e', 5))
        ).delete('d')
        tree.root.should == two(val('a', 1), 'b', 2, val2('c', 3, 'e', 5))
        value.should == 4
      end

      it 'should delete the first key in a parent of 3, 2 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'e', 5, val('f', 6))
        ).delete('c')
        tree.root.should == three(val('a', 1), 'b', 2, val('d', 4), 'e', 5, val('f', 6))
        value.should == 3
      end

      it 'should delete the second key in a parent of 3, 2 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'e', 5, val('f', 6))
        ).delete('e')
        tree.root.should == two(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'f', 6))
        value.should == 5
      end

      it 'should delete the first key in a parent of 2, 3 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val2('c', 3, 'd', 4), 'e', 5, val('f', 6))
        ).delete('b')
        tree.root.should == three(val('a', 1), 'c', 3, val('d', 4), 'e', 5, val('f', 6))
        value.should == 2
      end

      it 'should delete the second key in a parent of 2, 3 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val2('c', 3, 'd', 4), 'e', 5, val('f', 6))
        ).delete('e')
        tree.root.should == three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val('f', 6))
        value.should == 5
      end

      it 'should delete the first key in a parent of 2, 2 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val2('e', 5, 'f', 6))
        ).delete('b')
        tree.root.should == two(val2('a', 1, 'c', 3), 'd', 4, val2('e', 5, 'f', 6))
        value.should == 2
      end

      it 'should delete the second key in a parent of 2, 2 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val('c', 3), 'd', 4, val2('e', 5, 'f', 6))
        ).delete('d')
        tree.root.should == three(val('a', 1), 'b', 2, val('c', 3), 'e', 5, val('f', 6))
        value.should == 4
      end

      it 'should delete the first key in a parent of 3, 3 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5), 'f', 6, val('g', 7))
        ).delete('c')
        tree.root.should == three(val2('a', 1, 'b', 2), 'd', 4, val('e', 5), 'f', 6, val('g', 7))
        value.should == 3
      end

      it 'should delete the second key in a parent of 3, 3 and 2' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5), 'f', 6, val('g', 7))
        ).delete('f')
        tree.root.should == three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'e', 5, val('g', 7))
        value.should == 6
      end

      it 'should delete the first key in a parent of 3, 2 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'e', 5, val2('f', 6, 'g', 7))
        ).delete('c')
        tree.root.should == three(val('a', 1), 'b', 2, val('d', 4), 'e', 5, val2('f', 6, 'g', 7))
        value.should == 3
      end

      it 'should delete the second key in a parent of 3, 2 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'e', 5, val2('f', 6, 'g', 7))
        ).delete('e')
        tree.root.should == three(val2('a', 1, 'b', 2), 'c', 3, val('d', 4), 'f', 6, val('g', 7))
        value.should == 5
      end

      it 'should delete the first key in a parent of 2, 3 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val2('c', 3, 'd', 4), 'e', 5, val2('f', 6, 'g', 7))
        ).delete('b')
        tree.root.should == three(val('a', 1), 'c', 3, val('d', 4), 'e', 5, val2('f', 6, 'g', 7))
        value.should == 2
      end

      it 'should delete the second key in a parent of 2, 3 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val('a', 1), 'b', 2, val2('c', 3, 'd', 4), 'e', 5, val2('f', 6, 'g', 7))
        ).delete('e')
        tree.root.should == three(val('a', 1), 'b', 2, val2('c', 3, 'd', 4), 'f', 6, val('g', 7))
        value.should == 5
      end

      it 'should delete the first key in a parent of 3, 3 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5), 'f', 6, val2('g', 7, 'h', 8))
        ).delete('c')
        tree.root.should == three(val2('a', 1, 'b', 2), 'd', 4, val('e', 5), 'f', 6, val2('g', 7, 'h', 8))
        value.should == 3
      end

      it 'should delete the second key in a parent of 3, 3 and 3' do
        tree, value = Flow::TwoThreeTree::Map.new(
          three(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5), 'f', 6, val2('g', 7, 'h', 8))
        ).delete('f')
        tree.root.should == three(val2('a', 1, 'b', 2), 'c', 3, val2('d', 4, 'e', 5), 'g', 7, val('h', 8))
        value.should == 6
      end
    end


    describe 'exhaustive test' do

      # Generates all possible 2-3 trees of a given depth, with sequentially numbered keys.
      def generate(depth, count=0)
        if depth == 1
          [val(count, count), val2(count, count, count + 1, count + 1)]
        else
          [].tap do |trees|
            generate(depth - 1, count).each do |left|
              key = count + left.size
              generate(depth - 1, key + 1).each do |right|
                trees << two(left, key, key, right)
              end
            end

            generate(depth - 1, count).each do |left|
              key1 = count + left.size
              generate(depth - 1, key1 + 1).each do |middle|
                key2 = key1 + middle.size + 1
                generate(depth - 1, key2 + 1).each do |right|
                  trees << three(left, key1, key1, middle, key2, key2, right)
                end
              end
            end
          end
        end
      end

      (2..3).each do |depth|
        describe "all trees of depth #{depth}" do
          it 'should allow insertion at any position' do
            generate(depth).each do |root|
              tree = Flow::TwoThreeTree::Map.new(root)
              (0..tree.size).each do |index|
                new_tree = tree.set(index - 0.5, 'new')
                new_tree.root.check
                new_tree.size.should == tree.size + 1
              end
            end
          end

          it 'should allow deletion of any key' do
            generate(depth).each do |root|
              tree = Flow::TwoThreeTree::Map.new(root)
              (0...tree.size).each do |key|
                new_tree, value = tree.delete(key)
                new_tree.root.check
                new_tree.size.should == tree.size - 1
                value.should == key
              end
            end
          end
        end
      end
    end
  end
end
