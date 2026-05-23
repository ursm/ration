require 'test_helper'
require 'ration/backends/redis'
require 'securerandom'

class RedisBackendTest < Ration::Test
  POLL_INTERVAL = 0.05
  POP_TIMEOUT   = 5

  def redis_url
    ENV['RATION_TEST_REDIS_URL']
  end

  def setup
    super
    skip 'set RATION_TEST_REDIS_URL to run Redis tests' unless redis_url

    @channel = "ration_test_#{SecureRandom.hex(8)}"
    @backend = build_backend
  end

  def teardown
    @backend&.stop
    super
  end

  def build_backend(**overrides)
    Ration::Backends::Redis.new(
      url:           redis_url,
      channel:       @channel,
      poll_interval: POLL_INTERVAL,
      logger:        silent_logger,
      **overrides
    )
  end

  def test_publish_event_reaches_listener
    received = Queue.new
    @backend.on_event {|e| received.push(e) }
    @backend.start

    @backend.publish({n: 1, name: 'alice'})

    event = received.pop(timeout: POP_TIMEOUT)
    assert_equal({n: 1, name: 'alice'}, event)
  end

  def test_multiple_events_received_in_order
    received = Queue.new
    @backend.on_event {|e| received.push(e) }
    @backend.start

    3.times {|i| @backend.publish({n: i}) }

    assert_equal({n: 0}, received.pop(timeout: POP_TIMEOUT))
    assert_equal({n: 1}, received.pop(timeout: POP_TIMEOUT))
    assert_equal({n: 2}, received.pop(timeout: POP_TIMEOUT))
  end

  def test_payload_too_large_raises_before_publish
    small_backend = build_backend(max_payload_bytes: 50)
    small_backend.start

    begin
      assert_raises(Ration::PayloadTooLarge) {
        small_backend.publish({data: 'x' * 100})
      }
    ensure
      small_backend.stop
    end
  end

  def test_publish_with_hook_is_used_instead_of_default
    captured = []
    hub_backend = build_backend(
      publish_with: ->(channel, payload) { captured << [channel, payload] }
    )

    hub_backend.publish({n: 1})

    assert_equal 1, captured.size
    assert_equal @channel, captured.first.first
    assert_equal({n: 1}, JSON.parse(captured.first.last, symbolize_names: true))
  end

  def test_through_hub_fans_out_to_subscribers
    Ration.configure do |c|
      c.backend = build_backend
      c.logger  = silent_logger
    end

    received_a = Queue.new
    received_b = Queue.new

    thread_a = Thread.new {
      Ration.subscribe(max: 10) do |subscription|
        received_a.push(subscription.pop(timeout: POP_TIMEOUT))
      end
    }

    thread_b = Thread.new {
      Ration.subscribe(max: 10, filter: ->(e) { e[:topic] == 'foo' }) do |subscription|
        received_b.push(subscription.pop(timeout: POP_TIMEOUT))
      end
    }

    sleep(POLL_INTERVAL * 4)

    Ration.publish({topic: 'foo', n: 1})

    assert_equal({topic: 'foo', n: 1}, received_a.pop(timeout: POP_TIMEOUT))
    assert_equal({topic: 'foo', n: 1}, received_b.pop(timeout: POP_TIMEOUT))

    thread_a.join(POP_TIMEOUT)
    thread_b.join(POP_TIMEOUT)
  end

  def test_invalid_json_is_logged_and_skipped
    log     = StringIO.new
    logger  = Logger.new(log)
    backend = build_backend(logger: logger)

    received = Queue.new
    backend.on_event {|e| received.push(e) }
    backend.start

    begin
      client = RedisClient.new(url: redis_url)
      begin
        client.call('PUBLISH', @channel, 'not json')
        client.call('PUBLISH', @channel, '{"ok":true}')
      ensure
        client.close
      end

      event = received.pop(timeout: POP_TIMEOUT)
      assert_equal({ok: true}, event)
      assert_match(/invalid JSON/, log.string)
    ensure
      backend.stop
    end
  end
end
