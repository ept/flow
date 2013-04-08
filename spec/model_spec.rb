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


  describe 'mapping fields to methods' do
    describe 'for a versioned primitive field' do
      before do
        stub_const 'MyRecord', Flow::Model.new({
          'type' => 'record', 'name' => 'MyRecord', 'fields' => [
            {'name' => 'exampleField', 'type' => 'string'}
          ]
        })
      end

      it 'should create reader and writer methods' do
        record = MyRecord.new
        record.should be_respond_to(:example_field)
        record.should be_respond_to(:example_field=)

        Flow.transaction do
          record.example_field.should be_nil
          record.example_field = 'hello'
          record.example_field.should == 'hello'
        end
      end

      it 'should serialize and parse data' do
        Flow.transaction do
          record = MyRecord.new
          record.example_field = 'hello'
        end
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

      it 'should create reader and writer methods' do
        record = MyRecord.new
        record.should be_respond_to(:nested)
        record.should be_respond_to(:nested=)
        nested = MyRecord::Nested.new
        nested.should be_respond_to(:example_field)
        nested.should be_respond_to(:example_field=)

        Flow.transaction do
          record.nested = nested
          record.nested.example_field = 'hello'
          record.nested.example_field.should == 'hello'
        end
      end

      it 'should serialize and parse data' do
        Flow.transaction do
          record = MyRecord.new
          record.nested = MyRecord::Nested.new
          record.nested.example_field = 'hello'
        end

        parsed = MyRecord.parse(record.serialize, MyRecord::FLOW_SCHEMA)
        parsed.nested.example_field.should == 'hello'
      end
    end
  end
end
