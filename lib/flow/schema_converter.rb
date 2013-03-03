module Flow
  class SchemaConverter
    PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
    NAMED_TYPES = Set.new(%w[fixed enum record error])

    attr_reader :flow_schema

    def initialize(app_schema)
      app_schema = Yajl.load(app_schema) if app_schema.is_a? String
      @app_schema = app_schema
      @name_mapping = {}
      @versioned_types = {}
      @flow_schema = convert(app_schema)
    end

    private

    def convert(json_obj, namespace = nil, field_info = {:path => []})
      if json_obj.is_a? Hash
        type = json_obj['type']

        if PRIMITIVE_TYPES.include?(type)
          type

        elsif NAMED_TYPES.include?(type)
          named_type(json_obj, namespace)

        elsif type == 'array'
          case json_obj['semantics'] || 'list'
          when 'list' then list_schema(json_obj, namespace, field_info)
          when 'set'  then set_schema(json_obj, namespace, field_info)
          else raise Avro::SchemaParseError.new("Unknown array semantics: #{json_obj['semantics'].inspect}")
          end

        elsif type == 'map'
          map_schema(json_obj, namespace, field_info)
        else
          raise Avro::SchemaParseError.new("Unknown type: #{json_obj.inspect}")
        end

      elsif json_obj.is_a?(Array) # JSON array (union)
        json_obj.map {|schema| convert(schema, namespace, field_info) }

      elsif json_obj.is_a?(String)
        return json_obj if PRIMITIVE_TYPES.include?(json_obj)
        full_name = (json_obj =~ /\./) ? json_obj : [namespace, json_obj].compact.join('.')
        @name_mapping[full_name] or raise Avro::SchemaParseError.new("Unknown named type: #{full_name}")

      else
        raise Avro::SchemaParseError.new("Unknown schema: #{json_obj.inspect}")
      end
    end


    def named_type(json_obj, namespace)
      if !json_obj['name']
        raise Avro::SchemaParseError.new("Named type without a name: #{json_obj.inspect}")
      end

      name = json_obj['name'].split('.').last
      namespace = json_obj['name'][/\A(.*)\.[^\.]+\z/, 1] || json_obj['namespace'] || namespace
      full_name = [namespace, name].compact.join('.')

      if namespace && namespace.split('.').any? {|seg| seg.downcase =~ /\A_flow/ }
        raise Avro::SchemaParseError.new("Reserved namespace: #{namespace}")
      end
      if @name_mapping.include?(full_name)
        raise Avro::SchemaParseError.new("Duplicate definition of named type #{full_name}")
      end

      json_obj = json_obj.merge('name' => name, 'namespace' => namespace)
      if %w(record error).include?(json_obj['type']) && json_obj['semantics'] != 'atomic'
        json_obj = record_schema(json_obj)
      else
        @name_mapping[full_name] = full_name
      end
      json_obj
    end

    def record_schema(record)
      original_name = [record['namespace'], record['name']].compact.join('.')
      generated_namespace = [record['namespace'], '_flow_record'].compact.join('.')
      @name_mapping[original_name] = [generated_namespace, record['name']].join('.')

      fields = (record['fields'] || []).map do |field|
        field_schema(field, record['namespace'], record['name'])
      end
      fields.unshift(vector_clock_field)

      {
        'type' => 'record',
        'name' => record['name'],
        'namespace' => generated_namespace,
        'fields' => fields
      }.tap do |flow_record|
        flow_record['doc'] = record['doc'] if record.include?('doc')
      end
    end

    def field_schema(field, namespace, record_name)
      if !field['name']
        raise Avro::SchemaParseError.new("Field without a name: #{field.inspect}")
      end
      if field['name'] =~ /\A_flow/
        raise Avro::SchemaParseError.new("Reserved field name: #{field['name']}")
      end

      # field_info is passed into recursive conversion as a naming context for any
      # metadata record types we need to generate.
      field_info = {
        :record_name => record_name,
        :namespace => namespace,
        :path => [field['name']]
      }
      value_type = convert(field['type'], namespace, field_info)

      if value_type.is_a?(Array) && value_type.uniq.size == 2 &&
          value_type.include?('null') && !field['default']
        optional_type = (value_type - ['null']).first
      end

      wrapper_field = {'name' => field['name']}
      wrapper_field['doc']     = field['doc']     if field.include?('doc')
      wrapper_field['aliases'] = field['aliases'] if field.include?('aliases')

      if PRIMITIVE_TYPES.include?(value_type) && !field.include?('default')
        wrapper_field['type'] = 'com.flowprotocol.crdt.Versioned' + value_type.capitalize
      elsif PRIMITIVE_TYPES.include?(optional_type)
        wrapper_field['type'] = 'com.flowprotocol.crdt.Optional' + optional_type.capitalize
        wrapper_field['default'] = {'value' => nil, 'version' => nil}

      elsif @versioned_types.include?(value_type) && !field.include?('default')
        wrapper_field['type'] = @versioned_types[value_type]
      elsif @versioned_types.include?(optional_type)
        wrapper_field['type'] = ['null', @versioned_types[value_type]]
        wrapper_field['default'] = nil

      elsif value_type.is_a?(Hash) && NAMED_TYPES.include?(value_type['type'])
        wrapper_field['type'] = versioned_named_type(value_type)
      elsif optional_type.is_a?(Hash) && NAMED_TYPES.include?(optional_type['type'])
        wrapper_field['type'] = versioned_named_type(optional_type, :optional => true)
        wrapper_field['default'] = nil

      elsif (field['default'].is_a?(Hash) || field['default'].is_a?(Array)) && !field['default'].empty?
        raise Avro::SchemaParseError, "Unsupported default: #{field['default'].inspect}"

      else
        value_field = {'name' => 'value', 'type' => value_type}
        value_field['default'] = field['default'] if field.include?('default')
        versioned = {
          'type' => 'record',
          'name' => field['name'],
          'namespace' => [namespace, '_flow_field', record_name].compact.join('.'),
          'fields'    => [value_field, version_field]
        }
        versioned['doc'] = field['doc'] if field.include?('doc')
        wrapper_field['type'] = ['null', versioned]
        wrapper_field['default'] = nil
      end

      wrapper_field
    end

    def versioned_named_type(type, options={})
      versioned_namespace = (
        type['namespace'].split('.').reject{|seg| seg == '_flow_record' } + ['_flow_versioned']
      ).join('.')
      @versioned_types["#{type['namespace']}.#{type['name']}"] = "#{versioned_namespace}.#{type['name']}"

      versioned = {
        'type' => 'record',
        'name' => type['name'],
        'namespace' => versioned_namespace,
        'fields' => [
          {'name' => 'value',   'type' => ['null', type], 'default' => nil},
          version_field
        ]
      }
      options[:optional] ? ['null', versioned] : versioned
    end

    def list_schema(list, namespace, field_info)
      generated_namespace = (
        [namespace, '_flow_list', field_info[:record_name]] + field_info[:path]
      ).compact.join('.')
      value_field_info = field_info.merge(:path => field_info[:path] + ['list'])
      value_type = convert(list['items'], namespace, value_field_info)

      # Make sure the value type is a union, with null as one of the branches
      if value_type.is_a? Array
        value_type.unshift 'null' unless value_type.include? 'null'
      else
        value_type = ['null', value_type]
      end
      value_field = {'name' => 'value', 'type' => value_type}

      value_type_ref = type_definitions_to_references(value_type)

      {
        'type' => 'record',
        'name' => 'OrderedList',
        'namespace' => generated_namespace,
        'fields' => [
          vector_clock_field,
          {
            'name' => 'elements',
            'default' => [],
            'type' => {'type' => 'array', 'items' => {
              'type' => 'record',
              'name' => 'ListElement',
              'namespace' => generated_namespace,
              'fields' => [
                version_field('id'),
                version_field('valueVersion'),
                version_field('positionVersion'),
                {'name' => 'timestamp', 'type' => 'long'},
                {'name' => 'deleted', 'type' => 'boolean', 'default' => false},
                value_field
              ]
            }}
          },
          {
            'name' => 'queue',
            'default' => [],
            'type' => {'type' => 'array', 'items' => {
              'name' => 'ListOperation',
              'namespace' => generated_namespace,
              'type' => 'record',
              'fields' => [
                {'name' => 'writer', 'type' => 'com.flowprotocol.crdt.PeerID'},
                vector_clock_field('vectorClock'),
                {'name' => 'timestamp', 'type' => 'long'},
                {
                  'name' => 'modifications',
                  'type' => {'type' => 'array', 'items' => [
                    {
                      'type' => 'record',
                      'name' => 'ListInsert',
                      'namespace' => generated_namespace,
                      'fields' => [
                        version_field('precedingElement', :optional => true),
                        {'name' => 'value', 'type' => value_type_ref}
                      ]
                    },
                    {
                      'type' => 'record',
                      'name' => 'ListUpdate',
                      'namespace' => generated_namespace,
                      'fields' => [
                        version_field('elementID'),
                        {'name' => 'value', 'type' => value_type_ref}
                      ]
                    },
                    {
                      'type' => 'record',
                      'name' => 'ListDelete',
                      'namespace' => generated_namespace,
                      'fields' => [version_field('elementID')]
                    },
                    {
                      'type' => 'record',
                      'name' => 'ListReorder',
                      'namespace' => generated_namespace,
                      'fields' => [
                        version_field('elementID'),
                        version_field('precedingElement', :optional => true),
                      ]
                    }
                  ]}
                }
              ]
            }}
          }
        ]
      }
    end

    def set_schema(set, namespace, field_info)
      raise 'TODO'
    end

    def map_schema(map, namespace, field_info)
      raise 'TODO'
    end

    def vector_clock_field(name='_flowVectorClock')
      {
        'name' => name,
        'type' => {'type' => 'array', 'items' => 'com.flowprotocol.crdt.VectorClockEntry'},
        'default' => []
      }
    end

    def version_field(name='version', options={})
      type = 'com.flowprotocol.crdt.VectorClockVersion'
      type = ['null', type] if options[:optional]
      {'name' => name, 'type' => type}
    end

    # Takes a schema (parsed JSON) and transforms any inline definitions of named types with
    # references to those types. We assume here that any named type definitions are explicitly
    # annotated with their namespace.
    def type_definitions_to_references(json_obj)
      case json_obj
      when Hash
        type = json_obj['type']
        if PRIMITIVE_TYPES.include?(type)
          type
        elsif NAMED_TYPES.include?(type)
          [json_obj['namespace'], json_obj['name']].compact.join('.')
        elsif type == 'array' || type == 'map'
          {'type' => type, 'items' => type_definitions_to_references(json_obj['items'])}
        else
          raise Avro::SchemaParseError.new("Unknown type: #{json_obj.inspect}")
        end
      when Array
        json_obj.map(&method(:type_definitions_to_references))
      when String
        json_obj
      else
        raise Avro::SchemaParseError.new("Unknown schema: #{json_obj.inspect}")
      end
    end
  end
end
