# encoding: utf-8

module Cql
  # @private
  class ByteBuffer
    def initialize(initial_bytes='')
      @read_buffer = ''
      @write_buffer = ''
      @offset = 0
      @length = 0
      append(initial_bytes) unless initial_bytes.empty?
    end

    attr_reader :length
    alias_method :size, :length
    alias_method :bytesize, :length

    def empty?
      length == 0
    end

    def append(bytes)
      if bytes.is_a?(self.class)
        bytes.append_to(self)
      else
        @write_buffer << bytes
        @length += bytes.bytesize
      end
      self
    end
    alias_method :<<, :append

    def discard(n)
      raise RangeError, "#{n} bytes to discard but only #{@length} available" if @length < n
      @offset += n
      @length -= n
      self
    end

    def read(n)
      raise RangeError, "#{n} bytes required but only #{@length} available" if @length < n
      if @offset >= @read_buffer.bytesize
        swap_buffers
      end
      if @offset + n > @read_buffer.bytesize
        s = read(@read_buffer.bytesize - @offset)
        s << read(n - s.bytesize)
        s
      else
        s = @read_buffer[@offset, n]
        @offset += n
        @length -= n
        s
      end
    end

    def read_int
      raise RangeError, "4 bytes required to read an int, but only #{@length} available" if @length < 4
      read(4).unpack("N").first
    end

    def read_short
      raise RangeError, "2 bytes required to read a short, but only #{@length} available" if @length < 2
      read(2).unpack("n").first
    end

    def read_byte(signed=false)
      raise RangeError, "No bytes available to read byte" if empty?
      b = read(1)[0]
      b = (b & 0x7f) - (b & 0x80) if signed
      b
    end

    def update(location, bytes)
      absolute_offset = @offset + location
      bytes_length = bytes.bytesize
      if absolute_offset >= @read_buffer.bytesize
        @write_buffer[absolute_offset - @read_buffer.bytesize, bytes_length] = bytes
      else
        overflow = absolute_offset + bytes_length - @read_buffer.bytesize
        read_buffer_portion = bytes_length - overflow
        @read_buffer[absolute_offset, read_buffer_portion] = bytes[0, read_buffer_portion]
        if overflow > 0
          @write_buffer[0, overflow] = bytes[read_buffer_portion, bytes_length - 1]
        end
      end
    end

    def cheap_peek
      if @offset >= @read_buffer.bytesize
        swap_buffers
      end
      @read_buffer[@offset, @read_buffer.bytesize - @offset]
    end

    def eql?(other)
      self.to_str.eql?(other.to_str)
    end
    alias_method :==, :eql?

    def hash
      to_str.hash
    end

    def dup
      self.class.new(to_str)
    end

    def to_str
      (@read_buffer + @write_buffer)[@offset, @length]
    end
    alias_method :to_s, :to_str

    def inspect
      %(#<#{self.class.name}: #{to_str.inspect}>)
    end

    protected

    def append_to(other)
      other.raw_append(cheap_peek)
      other.raw_append(@write_buffer) unless @write_buffer.empty?
    end

    def raw_append(bytes)
      @write_buffer << bytes
      @length += bytes.bytesize
    end

    private

    def swap_buffers
      @offset -= @read_buffer.bytesize
      @read_buffer = @write_buffer
      @write_buffer = ''
    end
  end
end