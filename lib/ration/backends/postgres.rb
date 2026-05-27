require 'json'
require 'logger'
require 'pg'

module Ration
  module Backends
    class Postgres < Base
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
        initial_conn = connect_and_listen
        @thread = Thread.new { run_loop(initial_conn) }
      end

      def stop
        @stop = true
        @thread&.join
        @thread = nil
      end

      private

      def publish_direct(channel, payload)
        conn = PG.connect(@url)
        begin
          conn.exec_params('SELECT pg_notify($1, $2)', [channel, payload])
        ensure
          conn.close
        end
      end

      def connect_and_listen
        conn = PG.connect(@url)
        conn.exec("LISTEN #{conn.escape_identifier(@channel)}")
        conn
      end

      def run_loop(conn)
        current = conn

        until @stop
          begin
            listen_loop(current)
          rescue PG::Error => e
            @logger.error("Ration::Backends::Postgres listener error: #{e.class}: #{e.message}")
            current&.close
            current = reconnect_with_backoff
          end
        end

        current&.close
      end

      def reconnect_with_backoff
        backoff = INITIAL_BACKOFF_SECONDS

        until @stop
          sleep(backoff)
          break if @stop

          begin
            return connect_and_listen
          rescue PG::Error => e
            @logger.error("Ration::Backends::Postgres reconnect failed: #{e.class}: #{e.message}")
            backoff = [backoff * 2, MAX_BACKOFF_SECONDS].min
          end
        end

        nil
      end

      def listen_loop(conn)
        until @stop
          conn.wait_for_notify(@poll_interval) do |_channel, _pid, payload|
            begin
              event = JSON.parse(payload, symbolize_names: true)
              emit(event)
            rescue JSON::ParserError => e
              @logger.error("Ration::Backends::Postgres received invalid JSON: #{e.message}")
            end
          end
        end
      end
    end
  end
end
