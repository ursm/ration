require 'test_helper'

class HubTest < Ration::Test
  def build_hub(sync: true)
    backend = Ration::Backends::Memory.new(sync: sync)
    Ration::Hub.new(backend: backend, logger: silent_logger)
  end

  def test_publish_fans_out_to_all_subscribers
    hub = build_hub
    sub_a = hub.subscribe(max: 10)
    sub_b = hub.subscribe(max: 10)

    hub.publish({n: 1})

    assert_equal({n: 1}, sub_a.pop(timeout: 0))
    assert_equal({n: 1}, sub_b.pop(timeout: 0))
  end

  def test_filter_restricts_to_matching_events
    hub = build_hub
    sub = hub.subscribe(max: 10, filter: ->(e) { e[:topic] == :foo })

    hub.publish({topic: :foo, n: 1})
    hub.publish({topic: :bar, n: 2})

    assert_equal({topic: :foo, n: 1}, sub.pop(timeout: 0))
    assert_nil sub.pop(timeout: 0)
  end

  def test_block_form_auto_unsubscribes
    hub = build_hub

    hub.subscribe(max: 10) do |subscription|
      hub.publish({n: 1})

      assert_equal({n: 1}, subscription.pop(timeout: 0))
      assert_equal 1, hub.subscription_count
    end

    assert_equal 0, hub.subscription_count
  end

  def test_overflow_close_gcs_subscription
    hub = build_hub
    sub = hub.subscribe(max: 1, on_overflow: :close)

    hub.publish({n: 1})
    hub.publish({n: 2})

    assert sub.closed?
    assert_equal 0, hub.subscription_count
  end

  def test_externally_closed_subscription_is_gced_on_next_delivery
    hub = build_hub
    sub = hub.subscribe(max: 10)

    sub.close

    assert sub.closed?
    assert_equal 1, hub.subscription_count, 'no delivery has triggered GC yet'

    hub.publish({n: 1})

    assert_equal 0, hub.subscription_count
  end

  def test_filter_exception_does_not_break_other_subscribers
    hub = build_hub
    broken  = hub.subscribe(max: 10, filter: ->(_e) { raise 'boom' })
    healthy = hub.subscribe(max: 10)

    hub.publish({n: 1})

    assert broken.closed?
    assert_equal({n: 1}, healthy.pop(timeout: 0))
  end

  def test_payload_too_large_raises
    hub = build_hub
    assert_raises(Ration::PayloadTooLarge) {
      hub.publish({data: 'x' * 10_000})
    }
  end

  def test_stop_closes_all_subscriptions
    hub = build_hub
    sub = hub.subscribe(max: 10)

    hub.stop

    assert sub.closed?
    assert_equal 0, hub.subscription_count
  end
end
