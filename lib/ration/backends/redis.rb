require 'json'
require 'logger'
require 'redis-client'

module Ration
  module Backends
    class Redis < Base
      DEFAULT_CHANNEL         = 'ration'
      INITIAL_BACKOFF_SECONDS = 1
      MAX_BACKOFF_SECONDS     = 30
      DEFAULT_POLL_INTERVAL   = 1.0

      def initialize(
        url:,
        channel:           DEFAULT_CHANNEL,
        max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        poll_interval:     DEFAULT_POLL_INTERVAL,
        publish_with:      nil,
        logger:            nil
      )
        super()
        @url               = url
        @channel           = channel
        @max_payload_bytes = max_payload_bytes
        @poll_interval     = poll_interval
        @config            = RedisClient.config(url: url)
        @publish_with      = publish_with || method(:publish_direct)
        @logger            = logger || Logger.new($stderr)
        @thread            = nil
        @stop              = false
      end

      def publish(event)
        payload = event.to_json
        check_payload_size!(payload, @max_payload_bytes)

        @publish_with.call(@channel, payload)
      end

      def start
        return if @thread

        @stop = false
        initial = subscribe_client
        @thread = Thread.new { run_loop(initial) }
      end

      def stop
        @stop = true
        @thread&.join
        @thread = nil
      end

      private

      def publish_direct(channel, payload)
        client = @config.new_client
        begin
          client.call('PUBLISH', channel, payload)
        ensure
          client.close
        end
      end

      def subscribe_client
        client = @config.new_client
        pubsub = client.pubsub
        pubsub.call('SUBSCRIBE', @channel)
        pubsub
      end

      def run_loop(initial)
        current = initial

        until @stop
          begin
            listen_loop(current)
          rescue RedisClient::Error => e
            @logger.error("Ration::Backends::Redis listener error: #{e.class}: #{e.message}")
            close_quietly(current)
            current = reconnect_with_backoff
          end
        end

        close_quietly(current)
      end

      def reconnect_with_backoff
        backoff = INITIAL_BACKOFF_SECONDS

        until @stop
          sleep(backoff)
          break if @stop

          begin
            return subscribe_client
          rescue RedisClient::Error => e
            @logger.error("Ration::Backends::Redis reconnect failed: #{e.class}: #{e.message}")
            backoff = [backoff * 2, MAX_BACKOFF_SECONDS].min
          end
        end

        nil
      end

      def listen_loop(pubsub)
        until @stop
          event = pubsub.next_event(@poll_interval)
          next if event.nil?

          type, _channel, payload = event
          next unless type == 'message'

          begin
            parsed = JSON.parse(payload, symbolize_names: true)
            emit(parsed)
          rescue JSON::ParserError => e
            @logger.error("Ration::Backends::Redis received invalid JSON: #{e.message}")
          end
        end
      end

      def close_quietly(pubsub)
        pubsub&.close
      rescue RedisClient::Error
        # already broken; ignore
      end
    end
  end
end
