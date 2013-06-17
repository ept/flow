require 'spec_helper'

describe Flow::Model do

  describe 'creating model classes' do
    it 'should create an anonymous class' do
      klass = Flow::Model.new({'type' => 'record', 'name' => 'MyRecord', 'fields' => []})
      klass.should be_a(Class)
      klass.should < Flow::Model::RootModel
      klass.inspect.should =~ /#<Class:.*>/
    end

    it 'should place generated nested classes inside the root class' do
      stub_const 'MyRecord', Flow::Model.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'nested', 'type' => {
            'type' => 'record', 'name' => 'Nested', 'fields' => []
          }}
        ]
      })
      defined?(::MyRecord::Nested).should == 'constant'
      ::MyRecord::Nested.should < Flow::Model::Base
      ::MyRecord::Nested.to_s.should == 'MyRecord::Nested'
    end

    it 'should ignore namespace when generating class names' do
      stub_const 'MyRecord', Flow::Model.new({
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => 'my.name.space', 'fields' => [
          {'name' => 'nested1', 'type' => {
            'type' => 'record', 'name' => 'Nested', 'fields' => []
          }},
          {'name' => 'nested2', 'type' => 'my.name.space.Nested'}
        ]
      })
      defined?(MyRecord::Nested).should == 'constant'
      MyRecord::Nested.should < Flow::Model::Base
      defined?(MyRecord::MyNameSpaceNested).should be_nil
    end

    it 'should use namespaces to disambiguate name clashes' do
      stub_const 'MyRecord', Flow::Model.new({
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => 'my.name.space', 'fields' => [
          {'name' => 'nested1', 'type' => {
            'type' => 'record', 'name' => 'Nested', 'fields' => []
          }},
          {'name' => 'nested2', 'type' => {
            'type' => 'record', 'name' => 'Nested', 'namespace' => 'other.name.space', 'fields' => []
          }}
        ]
      })
      defined?(MyRecord::Nested).should be_nil
      defined?(MyRecord::MyNameSpaceNested).should == 'constant'
      MyRecord::MyNameSpaceNested.should < Flow::Model::Base
      defined?(MyRecord::OtherNameSpaceNested).should == 'constant'
      MyRecord::OtherNameSpaceNested.should < Flow::Model::Base
    end
  end

  describe 'modification safety' do
    before do
      stub_const 'MyRecord', Flow::Model.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'nested', 'type' => {
            'type' => 'record', 'name' => 'Nested', 'fields' => [
              {'name' => 'exampleField', 'type' => 'string'}
            ]
          }}
        ]
      })
    end

    it 'should not allow mutation outside of a transaction' do
      expect {
        MyRecord.new.nested = MyRecord::Nested.new
      }.to raise_error(/Modifications are only allowed in a transaction/)
    end

    it 'should not allow nested transactions' do
      expect {
        Flow.transaction { Flow.transaction { } }
      }.to raise_error(/already in a transaction/)
    end

    it 'should require traversing from the root in a transaction' do
      first = MyRecord.new
      second = Flow.transaction do
        first.nested = MyRecord::Nested.new
      end.updated(first)
      nested = second.nested
      expect {
        Flow.transaction { nested.example_field = 'foo' }
      }.to raise_error(/Please start from the root when accessing models inside a transaction/)
    end

    describe 'of accessor objects' do
      before do
        Flow.transaction do
          record = MyRecord.new
          record.nested = MyRecord::Nested.new
          @nested = record.nested
        end
      end

      it 'should not allow getters outside transactions' do
        expect {
          @nested.example_field
        }.to raise_error(/Accessors cannot be used outside of their transaction/)
      end

      it 'should not allow getters in another transaction' do
        expect {
          Flow.transaction { @nested.example_field }
        }.to raise_error(/Accessors cannot be used outside of their transaction/)
      end

      it 'should not allow setters outside transactions' do
        expect {
          @nested.example_field = 'foo'
        }.to raise_error(/Accessors cannot be used outside of their transaction/)
      end

      it 'should not allow setters in another transaction' do
        expect {
          Flow.transaction { @nested.example_field = 'foo' }
        }.to raise_error(/Accessors cannot be used outside of their transaction/)
      end
    end
  end

  describe 'mapping fields to methods' do
    describe 'for a versioned primitive field' do
      before do
        stub_const 'MyRecord', Flow::Model.new({
          'type' => 'record', 'name' => 'MyRecord', 'fields' => [
            {'name' => 'exampleField', 'type' => 'string'}
          ]
        })
      end

      it 'should allow safe modification of data' do
        record = MyRecord.new
        record.should be_respond_to(:example_field)
        record.should be_respond_to(:example_field=)

        updated_record = Flow.transaction do
          record.example_field.should be_nil
          record.example_field = 'hello'
          record.example_field.should == 'hello'
        end.updated(record)

        record.example_field.should be_nil
        updated_record.example_field.should == 'hello'
      end

      it 'should serialize and parse data' do
        record = Flow.transaction do
          record = MyRecord.new
          record.example_field = 'hello'
        end.updated(record)
        parsed = MyRecord.parse(record.serialize, MyRecord::FLOW_SCHEMA)
        parsed.example_field.should == 'hello'
      end
    end

    describe 'for a versioned field holding a nested record' do
      before do
        stub_const 'MyRecord', Flow::Model.new({
          'type' => 'record', 'name' => 'MyRecord', 'fields' => [
            {'name' => 'nested', 'type' => {
              'type' => 'record', 'name' => 'Nested', 'fields' => [
                {'name' => 'exampleField', 'type' => 'string'}
              ]
            }}
          ]
        })
      end

      it 'should allow safe modification of both outer and nested fields' do
        record = MyRecord.new
        nested = MyRecord::Nested.new

        updated = Flow.transaction do
          record.nested.should be_nil
          record.nested = nested
          record.nested.example_field.should be_nil
          record.nested.example_field = 'hello'
          record.nested.example_field.should == 'hello'
        end.updated(record)

        record.nested.should be_nil
        nested.example_field.should be_nil
        updated.nested.example_field.should == 'hello'
      end

      it 'should allow safe modification of outer fields' do
        first = MyRecord.new
        second = Flow.transaction do
          first.nested = MyRecord::Nested.new
          first.nested.example_field = 'hello'
        end.updated(first)

        third = MyRecord.new
        fourth = Flow.transaction do
          third.nested.should be_nil
          third.nested = second.nested
          third.nested.example_field.should == 'hello'
        end.updated(third)

        third.nested.should be_nil
        fourth.nested.example_field.should == 'hello'
      end

      it 'should allow safe modification of nested fields' do
        first = MyRecord.new
        second = Flow.transaction do
          first.nested = MyRecord::Nested.new
          first.nested.example_field = 'hello'
        end.updated(first)

        third = Flow.transaction do
          second.nested.example_field.should == 'hello'
          second.nested.example_field = 'world'
          second.nested.example_field.should == 'world'
        end.updated(second)

        second.nested.example_field.should == 'hello'
        third.nested.example_field.should == 'world'
      end

      it 'should serialize and parse data' do
        record = Flow.transaction do
          record = MyRecord.new
          record.nested = MyRecord::Nested.new
          record.nested.example_field = 'hello'
        end.updated(record)

        parsed = MyRecord.parse(record.serialize, MyRecord::FLOW_SCHEMA)
        parsed.nested.example_field.should == 'hello'
      end
    end
  end
end
