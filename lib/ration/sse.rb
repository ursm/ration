require 'json'

module Ration
  module SSE
    DEFAULT_ID_FROM = ->(event) { event[:id] }

    class << self
      def event(data:, event: nil, id: nil, retry_ms: nil)
        raise ArgumentError, 'data is required' if data.nil?

        lines = []

        if event
          ensure_field_value_safe!(event.to_s, 'event')
          lines << "event: #{event}"
        end

        if id
          ensure_field_value_safe!(id.to_s, 'id')
          lines << "id: #{id}"
        end

        unless retry_ms.nil?
          unless retry_ms.is_a?(Integer) && retry_ms >= 0
            raise ArgumentError, 'retry_ms must be a non-negative Integer'
          end

          lines << "retry: #{retry_ms}"
        end

        payload = data.is_a?(String) ? data : data.to_json
        payload.split("\n", -1).each do |line|
          lines << "data: #{line}"
        end

        lines.join("\n") + "\n\n"
      end

      def comment(text = '')
        ensure_no_newline!(text.to_s, 'comment')
        ": #{text}\n\n"
      end

      def ping
        ": ping\n\n"
      end

      def stream(
        subscription,
        output,
        heartbeat: 15,
        since:     nil,
        id_from:   DEFAULT_ID_FROM
      )
        raise ArgumentError, 'block required' unless block_given?

        last = since

        subscription.each_event(timeout: heartbeat) do |event|
          if event.nil?
            output << ping if heartbeat
            next
          end

          if last
            id = id_from.call(event)
            next if id.nil? || id <= last

            last = id
          end

          framed = yield(event)
          output << framed if framed
        end

        last
      end

      private

      def ensure_field_value_safe!(value, field)
        if value.match?(/[\r\n\0]/)
          raise ArgumentError, "#{field} must not contain newlines or NULL characters"
        end
      end

      def ensure_no_newline!(value, field)
        if value.match?(/[\r\n]/)
          raise ArgumentError, "#{field} must not contain newlines"
        end
      end
    end
  end
end
