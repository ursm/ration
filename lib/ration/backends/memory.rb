require 'json'
require 'logger'

module Ration
  module Backends
    class Memory < Base
      def initialize(max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES, sync: false, logger: nil)
        super()
        @max_payload_bytes = max_payload_bytes
        @sync              = sync
        @logger            = logger || Logger.new($stderr)
        @queue             = Queue.new
        @thread            = nil
      end

      def publish(event)
        check_payload_size!(event.to_json, @max_payload_bytes)

        if @sync
          emit(event)
        else
          @queue.push(event)
        end
      end

      def start
        return if @sync
        return if @thread

        @thread = Thread.new {
          while (event = @queue.pop)
            begin
              emit(event)
            rescue => e
              @logger.error("Ration::Backends::Memory listener error: #{e.class}: #{e.message}")
            end
          end
        }
      end

      def stop
        return if @sync

        @queue.close
        @thread&.join
        @queue  = Queue.new
        @thread = nil
      end
    end
  end
end
