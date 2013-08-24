# encoding: utf-8

module Cql
  module Protocol
    class PreparedResultResponse < ResultResponse
      attr_reader :id, :metadata

      def initialize(*args)
        @id, @metadata = args
      end

      def self.decode!(buffer)
        id = read_short_bytes!(buffer)
        metadata = RowsResultResponse.read_metadata!(buffer)
        new(id, metadata)
      end

      def to_s
        %(RESULT PREPARED #{id.each_byte.map { |x| x.to_s(16) }.join('')} #{@metadata.inspect})
      end
    end
  end
end
