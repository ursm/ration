module Ration
  module Backends
    class Base
      DEFAULT_MAX_PAYLOAD_BYTES = 6 * 1024

      def publish(event)
        raise NotImplementedError
      end

      def on_event(&block)
        @on_event = block
      end

      def start
        raise NotImplementedError
      end

      def stop
        raise NotImplementedError
      end

      protected

      def emit(event)
        @on_event&.call(event)
      end

      def check_payload_size!(payload, limit)
        return if payload.bytesize <= limit

        raise PayloadTooLarge, "Payload is #{payload.bytesize} bytes, max is #{limit}"
      end
    end
  end
end
