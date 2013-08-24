# encoding: utf-8

module Cql
  module Protocol
    class CredentialsRequest < Request
      attr_reader :credentials

      def initialize(credentials)
        super(4)
        @credentials = credentials.dup.freeze
      end

      def write(io)
        write_string_map(io, @credentials)
      end

      def to_s
        %(CREDENTIALS #{@credentials.inspect})
      end

      def eql?(rq)
        self.class === rq && rq.credentials.eql?(@credentials)
      end
      alias_method :==, :eql?

      def hash
        @h ||= @credentials.hash
      end
    end
  end
end
