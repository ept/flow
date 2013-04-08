require 'avro'
require 'openssl'
require 'flow/schema_converter'
require 'flow/model'
require 'flow/transaction'
require 'flow/two_three_tree'

module Flow
  class << self
    def peer_id
      @peer_id ||= OpenSSL::Random.random_bytes(8)
    end

    def peer_id=(peer_id)
      @peer_id = peer_id
    end
  end
end
