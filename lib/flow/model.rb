module Avro
  class Schema
    class RecordSchema
      attr_accessor :model_class
    end
  end
end

module Flow
  module Model
    class << self
      # Dynamically generates Ruby classes from an Avro schema, inspired by `Struct.new`:
      #
      #   MyRecord = Flow::Model.new(File.read('my_record.avsc'))
      #
      def new(schema)
        Flow::Model::Builder.new(schema).root_class
      end
    end

    class Builder
      attr_reader :app_schema, :flow_schema, :root_class, :class_by_name

      def initialize(schema)
        @app_schema  = Avro::Schema.real_parse(schema.is_a?(String) ? Yajl.load(schema) : schema, {})
        @flow_schema = Avro::Schema.real_parse(SchemaConverter.new(schema).flow_schema, flow_named_types)
        unless app_schema.is_a? Avro::Schema::RecordSchema
          raise ArgumentError, "The Flow Avro schema should have a record type at the root"
        end

        @class_by_name = {}
        @root_class = Class.new(Flow::Model::RootModel)
        create_types(app_schema, flow_schema)
      end

      private

      def flow_named_types
        unless @flow_named_types
          @flow_named_types = {}
          schema_file = File.expand_path('../../crdt_replica.avsc', File.dirname(__FILE__))
          Avro::Schema.real_parse(Yajl.load(File.read(schema_file)), @flow_named_types)
        end
        @flow_named_types.dup
      end

      # Creates a new subclass of Avro::IO::DatumReader that instantiates models when parsing
      # serialised Avro data.
      def make_datum_reader(model_class)
        Class.new(Avro::IO::DatumReader) do |klass|
          define_method(:model_class) { model_class }

          def read_data(writers_schema, readers_schema, decoder)
            model_class.instantiate_models(readers_schema, super)
          end
        end
      end

      # Recursively creates Ruby classes for all record types in the app schema.
      def create_types(app_schema, flow_schema)
        case app_schema
        when Avro::Schema::RecordSchema
          if flow_schema.namespace.split('.').include? '_flow_record'
            create_versioned_type(app_schema, flow_schema)
          else
            create_atomic_type(app_schema, flow_schema)
          end

          app_schema.fields.each do |app_field|
            flow_field = flow_schema.fields.detect{|field| field.name == app_field.name }
            create_types(app_field.type, skip_flow_types(flow_field.type))
          end

        when Avro::Schema::ArraySchema
          create_types(app_schema.items, TODO)
        when Avro::Schema::MapSchema
          create_types(app_schema.values, TODO)
        when Avro::Schema::UnionSchema
          app_schema.schemas.each {|schema| create_types(schema, TODO) }
        end
      end

      def skip_flow_types(flow_schema)
        return flow_schema unless flow_schema.is_a? Avro::Schema::RecordSchema
        namespace = flow_schema.namespace.split('.')

        if namespace.include?('_flow_versioned')
          value_type = flow_schema.fields.detect{|field| field.name == 'value' }.type
          if value_type.is_a? Avro::Schema::UnionSchema
            value_type = value_type.schemas.detect{|branch| branch.is_a? Avro::Schema::RecordSchema }
          end
          skip_flow_types(value_type)

        else
          flow_schema
        end
      end

      def create_versioned_type(app_schema, flow_schema)
        if class_by_name.include? app_schema.fullname
          app_schema.model_class = class_by_name[app_schema.fullname]
          flow_schema.model_class = class_by_name[app_schema.fullname]
          return
        end

        if app_schema.equal?(self.app_schema)
          # This is the root type, which will be assinged to a constant by the application.
          # Leave the generated class anonymous for now.
          klass = root_class
        else
          class_name = camelcase(app_schema.name)

          # Not using const_defined? because that searches outer/global constants too.
          if root_class.constants.any? {|const| const.to_s == class_name }
            conflicting = root_class.send(:remove_const, class_name)
            conflicting_name = conflicting.const_get(:APP_SCHEMA).fullname
            root_class.const_set(camelcase(conflicting_name), conflicting)
            class_name = camelcase(app_schema.fullname)
          end

          klass = Class.new(Flow::Model::Base)
          # The name of the class is the first constant that refers to it.
          root_class.const_set(class_name, klass)
        end

        class_by_name[app_schema.fullname] = klass
        app_schema.model_class = klass
        flow_schema.model_class = klass

        klass.const_set :APP_SCHEMA, app_schema
        klass.const_set :FLOW_SCHEMA, flow_schema
        klass.const_set :DatumReader, make_datum_reader(klass)
        class << klass
          define_method(:app_schema) { const_get(:APP_SCHEMA) }
          define_method(:flow_schema) { const_get(:FLOW_SCHEMA) }
          define_method(:datum_reader_class) { const_get(:DatumReader) }
        end

        app_schema.fields.each do |app_field|
          flow_field = flow_schema.fields.detect{|field| field.name == app_field.name }
          raise "Field #{app_schema.fullname}.#{app_field.name} does not appear in flow schema" unless flow_field
          create_accessors(klass, app_field, flow_field)
        end
      end

      # Create friendly accessor methods for Avro record fields
      def create_accessors(klass, app_field, flow_field)
        unless flow_schema.is_a? Avro::Schema::RecordSchema
          create_unversioned_accessors(klass, app_field, flow_field)
          return
        end

        namespace = (flow_field.type.namespace || '').split('.')

        if namespace.include?('_flow_versioned') || flow_field.type.fullname =~ /\Acom\.flowprotocol\.crdt\.(Versioned|Optional)/
          create_versioned_accessors(klass, app_field, flow_field)
        else
          create_unversioned_accessors(klass, app_field, flow_field)
        end
      end

      def create_unversioned_accessors(klass, app_field, flow_field)
        method_name = underscore(app_field.name).to_sym
        klass.class_eval do
          define_method(method_name) { @record[app_field.name] }
          define_method(:"#{method_name}=") {|value| @record[app_field.name] = value }
        end
      end

      def create_versioned_accessors(klass, app_field, flow_field)
        method_name = underscore(app_field.name).to_sym
        klass.class_eval do
          define_method(method_name) do
            versioned = @record[app_field.name]
            versioned && versioned['value']
          end

          define_method(:"#{method_name}=") do |value|
            @record[app_field.name] = {'version' => local_operation, 'value' => value}
          end
        end
      end

      # 'foo_bar_baz' => 'FooBarBaz'
      def camelcase(name)
        name.gsub(/(?:^|[^a-z0-9]+)([a-z0-9]+)/i) { $1.capitalize }
      end

      # 'FooBarBaz' => 'foo_bar_baz'
      def underscore(name)
        name.
          gsub(/([A-Z0-9]+)([A-Z][a-z])/, '\1_\2').
          gsub(/([a-z0-9])([A-Z])/, '\1_\2').
          gsub(/[^a-z0-9]+/i, '_').
          downcase
      end
    end

    # Base class for all classes dynamically created by Flow::Model.new.
    # Don't subclass it directly, use Flow::Model.new instead.
    class Base
      # record is a hash of field name => value
      def initialize(record={})
        raise ArgumentError, "expected Hash, got #{record.inspect}" unless record.is_a? Hash
        @record = record
      end

      def to_avro(record=@record)
        record.each_with_object({}) do |(key, value), new_hash|
          new_hash[key] = if value.respond_to?:to_avro
                            value.to_avro
                          elsif value.is_a? Hash
                            to_avro(value)
                          else
                            value
                          end
        end
      end

      private

      def local_operation
        vector = (@record['_flowVectorClock'] ||= [])
        if this_peer = vector.detect {|entry| entry['peerID'] == Flow.peer_id }
          this_peer['count'] += 0
        else
          vector << {'peerID' => Flow.peer_id, 'count' => 1}
        end

        {
          'lastWriterID' => Flow.peer_id,
          'vectorClockSum' => vector.inject(0) {|sum, entry| sum + entry['count'] }
        }
      end
    end

    # Superclass for the class dynamically created by Flow::Model.new for the record at the top of the
    # schema. Don't subclass it directly, use Flow::Model.new instead.
    class RootModel < Base
      class << self
        # Parses an instance of this record from its binary-serialized form, given as a string or a
        # Ruby IO object. Must be given the exact schema with which the serialized data was written.
        def parse(serialized, writer_schema)
          serialized = StringIO.new(serialized) unless serialized.respond_to? :read
          decoder = Avro::IO::BinaryDecoder.new(serialized)
          datum_reader = datum_reader_class.new(writer_schema, flow_schema)
          new(datum_reader.read(decoder))
        end

        # Intercepts object creation during Avro parsing
        def instantiate_models(schema, value)
          if schema.type == 'record' && schema.model_class
            if value.is_a? schema.model_class
              value # we get called twice on unions due to schema resolution; only instantiate models once
            elsif schema.model_class < RootModel
              value # allow subclassing of the root model
            else
              schema.model_class.new(value)
            end
          else
            value
          end
        end
      end

      def serialize(encoder=nil)
        use_encoder = encoder || Avro::IO::BinaryEncoder.new(StringIO.new)
        Avro::IO::DatumWriter.new(self.class.flow_schema).write(to_avro, use_encoder)
        use_encoder.writer.string unless encoder
      end
    end
  end
end
