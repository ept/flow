require 'spec_helper'

describe Flow::SchemaConverter do
  def vector_clock_field(name='_flowVectorClock')
    {
      'name' => name,
      'type' => {'type' => 'array', 'items' => 'com.flowprotocol.crdt.VectorClockEntry'},
      'default' => []
    }
  end

  describe 'mapping primitive types' do
    it 'should map primitive types to themselves' do
      Flow::SchemaConverter.new({'type' => 'string'}).flow_schema.should == 'string'
    end

    it 'should map a required primitive field to a versioned type' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'myField', 'type' => 'long'}
        ]
      }).flow_schema.should == {
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => '_flow_record',
        'fields' => [
          vector_clock_field,
          {'name' => 'myField', 'type' => 'com.flowprotocol.crdt.VersionedLong'}
        ]
      }
    end

    it 'should map an optional primitive field to an optional versioned type' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'myField', 'type' => ['null', 'bytes'], 'default' => nil}
        ]
      }).flow_schema.should == {
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => '_flow_record',
        'fields' => [
          vector_clock_field,
          {'name' => 'myField', 'type' => 'com.flowprotocol.crdt.OptionalBytes', 'default' => {'value' => nil, 'version' => nil}}
        ]
      }
    end
  end


  describe 'mapping nested named types' do
    it 'should map a required field of record type to a versioned edition of that record' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'myField', 'type' => {
            'type' => 'record', 'name' => 'NestedRecord', 'fields' => [
              {'name' => 'nested', 'type' => 'long'}
            ]
          }}
        ]
      }).flow_schema['fields'][1].tap do |field|
        field['name'].should == 'myField'
        field['type'].should == {
          'type' => 'record', 'name' => 'NestedRecord', 'namespace' => '_flow_versioned', 'fields' => [
            {'name' => 'value', 'default' => nil, 'type' => ['null', {
              'type' => 'record', 'name' => 'NestedRecord', 'namespace' => '_flow_record', 'fields' => [
                vector_clock_field,
                {'name' => 'nested', 'type' => 'com.flowprotocol.crdt.VersionedLong'}
              ]
            }]},
            {'name' => 'version', 'type' => 'com.flowprotocol.crdt.VectorClockVersion'}
          ]
        }
      end
    end

    it 'should map an optional field of record type to a versioned edition of that record' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'myField', 'default' => nil, 'type' => ['null', {
            'type' => 'record', 'name' => 'NestedRecord', 'fields' => [
              {'name' => 'nested', 'type' => 'long'}
            ]
          }]}
        ]
      }).flow_schema['fields'][1].tap do |field|
        field.should include('name' => 'myField', 'default' => nil)
        field['type'].first.should == 'null'
        field['type'].last.tap do |versioned|
          versioned.should include('name' => 'NestedRecord', 'namespace' => '_flow_versioned')
          versioned['fields'].first.should include('name' => 'value', 'default' => nil)
          versioned['fields'].first['type'].first.should == 'null'
          versioned['fields'].first['type'].last.should include('name' => 'NestedRecord', 'namespace' => '_flow_record')
          versioned['fields'].last.should include('name' => 'version', 'type' => 'com.flowprotocol.crdt.VectorClockVersion')
        end
      end
    end

    it 'should map two fields of the same nested record to the same versioned edition' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'field1', 'type' => {
            'type' => 'record', 'name' => 'NestedRecord', 'fields' => [
              {'name' => 'nested', 'type' => 'long'}
            ]
          }},
          {'name' => 'field2', 'type' => 'NestedRecord'}
        ]
      }).flow_schema.tap do |schema|
        schema['fields'][1].tap do |field1|
          field1.should include('name' => 'field1')
          field1['type'].should include('name' => 'NestedRecord', 'namespace' => '_flow_versioned')
        end
        schema['fields'][2].tap do |field2|
          field2.should include('name' => 'field2', 'type' => '_flow_versioned.NestedRecord')
        end
      end
    end

    it 'should map an enum type to a versioned edition of that enum' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'polliceVerso', 'type' => {
            'type' => 'enum', 'name' => 'PolliceVerso', 'symbols' => ['THUMBS_UP', 'THUMBS_DOWN']
          }}
        ]
      }).flow_schema['fields'][1].should == {
        'name' => 'polliceVerso', 'type' => {
          'type' => 'record', 'name' => 'PolliceVerso', 'namespace' => '_flow_versioned', 'fields' => [
            {'name' => 'value', 'default' => nil, 'type' => ['null', {
              # Not sure if a null namespace is valid, but we can't omit it, otherwise it would
              # inherit the parent namespace -- which would not be correct.
              'type' => 'enum', 'name' => 'PolliceVerso', 'namespace' => nil,
              'symbols' => ['THUMBS_UP', 'THUMBS_DOWN']
            }]},
            {'name' => 'version', 'type' => 'com.flowprotocol.crdt.VectorClockVersion'}
          ]
        }
      }
    end

    it 'should map two fields of the same enum type to the same versioned edition' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'rightHand', 'type' => ['null', {
            'type' => 'enum', 'name' => 'PolliceVerso', 'symbols' => ['THUMBS_UP', 'THUMBS_DOWN']
          }]},
          {'name' => 'leftHand', 'type' => ['null', 'PolliceVerso'], 'default' => nil}
        ]
      }).flow_schema.tap do |schema|
        schema['fields'][1].tap do |field1|
          field1.should include('name' => 'rightHand')
          field1['type'].first.should == 'null'
          field1['type'].last.should include('type' => 'record', 'name' => 'PolliceVerso', 'namespace' => '_flow_versioned')
        end
        schema['fields'][2].tap do |field2|
          field2.should include('name' => 'leftHand', 'type' => ['null', '_flow_versioned.PolliceVerso'])
        end
      end
    end

    it 'should map a fixed type to a versioned edition of that fixed' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'hash', 'type' => {
            'type' => 'fixed', 'name' => 'SHA1', 'size' => 20
          }}
        ]
      }).flow_schema['fields'][1].should == {
        'name' => 'hash', 'type' => {
          'type' => 'record', 'name' => 'SHA1', 'namespace' => '_flow_versioned', 'fields' => [
            {'name' => 'value', 'default' => nil, 'type' => ['null', {
              # Not sure if a null namespace is valid, but we can't omit it, otherwise it would
              # inherit the parent namespace -- which would not be correct.
              'type' => 'fixed', 'name' => 'SHA1', 'namespace' => nil, 'size' => 20
            }]},
            {'name' => 'version', 'type' => 'com.flowprotocol.crdt.VectorClockVersion'}
          ]
        }
      }
    end
  end


  describe 'mapping complex fields' do
    it 'should generate a field record for a field with a non-null default value' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'answer', 'type' => 'long', 'default' => 42}
        ]
      }).flow_schema.should == {
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => '_flow_record',
        'fields' => [
          vector_clock_field,
          {'name' => 'answer', 'default' => nil, 'type' => ['null', {
            'type' => 'record', 'name' => 'answer', 'namespace' => '_flow_field.MyRecord', 'fields' => [
              {'name' => 'value', 'type' => 'long', 'default' => 42},
              {'name' => 'version', 'type' => 'com.flowprotocol.crdt.VectorClockVersion'}
            ]
          }]}
        ]
      }
    end

    it 'should generate a field record for a field with multiple non-null union branches' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'someField', 'type' => [
            {'type' => 'record', 'name' => 'Branch1', 'fields' => []},
            {'type' => 'record', 'name' => 'Branch2', 'fields' => [
              {'name' => 'otherField', 'type' => 'long'}
            ]}
          ]}
        ]
      }).flow_schema['fields'][1].tap do |field|
        field.should include('name' => 'someField', 'default' => nil)
        field['type'].size.should == 2
        field['type'].first.should == 'null'
        field['type'].last.tap do |field_type|
          field_type.should include('type' => 'record', 'name' => 'someField', 'namespace' => '_flow_field.MyRecord')
          field_type['fields'].map{|f| f['name'] }.should == ['value', 'version']
          field_type['fields'].first['type'].tap do |union_type|
            union_type.size.should == 2
            union_type.first.should include('type' => 'record', 'name' => 'Branch1', 'namespace' => '_flow_record')
            union_type.last.should include('type' => 'record', 'name' => 'Branch2', 'namespace' => '_flow_record')
          end
        end
      end
    end

    it 'should generate an ordered list construction for a field with array type' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'characters', 'type' => {'type' => 'array', 'items' => 'string'}}
        ]
      }).flow_schema['fields'][1].tap do |field|
        field.should include('name' => 'characters', 'default' => nil)
        field['type'].first.should == 'null'
        field['type'].last.tap do |list_type|
          list_type.should include('name' => 'OrderedList', 'namespace' => '_flow_list.MyRecord.characters')
          list_type['fields'].map{|field| field['name'] }.should == %w(_flowVectorClock elements queue)
          list_type['fields'][1]['type']['items'].tap do |element|
            element.should include('type' => 'record', 'name' => 'ListElement', 'namespace' => '_flow_list.MyRecord.characters')
            element['fields'].map{|field| field['name'] }.should == %w(id valueVersion positionVersion timestamp deleted value)
            element['fields'].last['type'].should == ['null', 'string']
          end
          list_type['fields'][2]['type']['items'].tap do |operation|
            operation.should include('type' => 'record', 'name' => 'ListOperation', 'namespace' => '_flow_list.MyRecord.characters')
            operation['fields'].map{|field| field['name'] }.should == %w(writer vectorClock timestamp modifications)
            operation['fields'].last['type']['items'].tap do |modification|
              modification.map{|mod| mod['name'] }.should == %w(ListInsert ListUpdate ListDelete ListReorder)
              modification.each{|mod| mod['namespace'].should == '_flow_list.MyRecord.characters' }
              modification[0]['fields'].detect{|f| f['name'] == 'value' }['type'].should == 'string'
              modification[1]['fields'].detect{|f| f['name'] == 'value' }['type'].should == 'string'
            end
          end
        end
      end
    end

    it 'should support nested ordered lists' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'fields' => [
          {'name' => 'matrix', 'type' => {'type' => 'array', 'items' => {
            'type' => 'array', 'items' => {
              'type' => 'record', 'name' => 'Item', 'fields' => [
                {'name' => 'value', 'type' => 'long'}
              ]
            }
          }}}
        ]
      }).flow_schema['fields'][1]['type'].last.tap do |outer_list|
        outer_list.should include('name' => 'OrderedList', 'namespace' => '_flow_list.MyRecord.matrix')
        outer_list['fields'].detect{|f| f['name'] == 'elements' }['type']['items'].tap do |outer_element|
          outer_element.should include('name' => 'ListElement', 'namespace' => '_flow_list.MyRecord.matrix')
          outer_element['fields'].detect{|f| f['name'] == 'value' }['type'].last.tap do |inner_list|
            inner_list.should include('name' => 'OrderedList', 'namespace' => '_flow_list.MyRecord.matrix.list')
            inner_list['fields'].detect{|f| f['name'] == 'elements' }['type']['items'].tap do |inner_element|
              inner_element.should include('name' => 'ListElement', 'namespace' => '_flow_list.MyRecord.matrix.list')
              inner_element['fields'].detect{|f| f['name'] == 'value' }['type'].last.tap do |value_record|
                value_record.should include('name' => 'Item', 'namespace' => '_flow_record')
              end
            end
          end
        end
      end
    end
  end


  describe 'namespace handling' do
    it 'should place converted record types into a sub-namespace (inline namespace)' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'my.name.space.MyRecord', 'fields' => [
          {'name' => 'myField', 'type' => 'long'}
        ]
      }).flow_schema.should include(
        'namespace' => 'my.name.space._flow_record',
        'name'      => 'MyRecord'
      )
    end

    it 'should place converted record types into a sub-namespace (namespace in a separate field)' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => 'my.name.space', 'fields' => [
          {'name' => 'myField', 'type' => 'long'}
        ]
      }).flow_schema.should include(
        'namespace' => 'my.name.space._flow_record',
        'name'      => 'MyRecord'
      )
    end

    it 'should default nested types to be in their parent\'s namespace' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'my.name.space.MyRecord', 'fields' => [
          {'name' => 'myField', 'type' => {
            'type' => 'record', 'name' => 'NestedRecord', 'fields' => []
          }}
        ]
      }).flow_schema.tap do |schema|
        schema.should include('name' => 'MyRecord', 'namespace' => 'my.name.space._flow_record')
        schema['fields'][1]['type'].tap do |versioned|
          versioned.should include('name' => 'NestedRecord', 'namespace' => 'my.name.space._flow_versioned')
          versioned['fields'].first['type'].tap do |union_type|
            union_type.last.should include('name' => 'NestedRecord', 'namespace' => 'my.name.space._flow_record')
          end
        end
      end
    end

    it 'should allow nested types to be in a different namespace' do
      Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => 'outer', 'fields' => [
          {'name' => 'myField', 'type' => {
            'type' => 'record', 'name' => 'NestedRecord', 'namespace' => 'inner', 'fields' => []
          }}
        ]
      }).flow_schema.tap do |schema|
        schema.should include('name' => 'MyRecord', 'namespace' => 'outer._flow_record')
        schema['fields'][1]['type'].tap do |versioned|
          versioned.should include('name' => 'NestedRecord', 'namespace' => 'inner._flow_versioned')
          versioned['fields'].first['type'].tap do |union_type|
            union_type.last.should include('name' => 'NestedRecord', 'namespace' => 'inner._flow_record')
          end
        end
      end
    end

    it 'should keep enum types in their original namespace' do
      schema = Flow::SchemaConverter.new({
        'type' => 'record', 'name' => 'MyRecord', 'namespace' => 'my.name.space', 'fields' => [
          {'name' => 'polliceVerso', 'type' => {
            'type' => 'enum', 'name' => 'PolliceVerso', 'symbols' => ['THUMBS_UP', 'THUMBS_DOWN']
          }}
        ]
      }).flow_schema.tap do |schema|
        schema.should include('name' => 'MyRecord', 'namespace' => 'my.name.space._flow_record')
        schema['fields'][1]['type'].tap do |versioned|
          versioned.should include('name' => 'PolliceVerso', 'namespace' => 'my.name.space._flow_versioned')
          versioned['fields'].first['type'].tap do |union_type|
            union_type.last.should include('type' => 'enum', 'name' => 'PolliceVerso', 'namespace' => 'my.name.space')
          end
        end
      end
    end
  end
end
