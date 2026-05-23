require 'test_helper'

class SubscriptionTest < Ration::Test
  def build(max: 10, filter: nil, on_overflow: :close)
    Ration::Subscription.new(
      max:         max,
      filter:      filter,
      on_overflow: on_overflow,
      logger:      silent_logger
    )
  end

  def test_delivers_events_matching_filter
    sub = build(filter: ->(e) { e[:user_id] == 1 })
    sub.deliver({user_id: 1, body: 'hi'})
    sub.deliver({user_id: 2, body: 'no'})

    assert_equal({user_id: 1, body: 'hi'}, sub.pop(timeout: 0))
    assert_nil sub.pop(timeout: 0)
  end

  def test_delivers_all_events_when_no_filter
    sub = build
    sub.deliver({a: 1})
    sub.deliver({a: 2})

    assert_equal({a: 1}, sub.pop(timeout: 0))
    assert_equal({a: 2}, sub.pop(timeout: 0))
  end

  def test_filter_exception_closes_subscription
    sub = build(filter: ->(_e) { raise 'boom' })
    sub.deliver({a: 1})

    assert sub.closed?, 'subscription should be closed after filter exception'
  end

  def test_overflow_close_closes_subscription
    sub = build(max: 1, on_overflow: :close)
    sub.deliver({a: 1})
    sub.deliver({a: 2})

    assert sub.closed?, 'subscription should close on overflow when policy is :close'
    assert_equal({a: 1}, sub.pop(timeout: 0))
  end

  def test_overflow_drop_oldest_keeps_newest
    sub = build(max: 1, on_overflow: :drop_oldest)
    sub.deliver({a: 1})
    sub.deliver({a: 2})
    sub.deliver({a: 3})

    refute sub.closed?, 'subscription should stay alive on overflow when policy is :drop_oldest'
    assert_equal({a: 3}, sub.pop(timeout: 0))
  end

  def test_deliver_after_close_is_noop
    sub = build
    sub.close
    sub.deliver({a: 1})

    assert sub.closed?
  end

  def test_unknown_overflow_policy_raises
    assert_raises(ArgumentError) {
      build(on_overflow: :bogus)
    }
  end

  def test_has_id
    sub = build
    assert_kind_of String, sub.id
    refute_equal build.id, sub.id
  end

  def test_each_event_yields_until_closed
    sub = build
    sub.deliver({a: 1})
    sub.deliver({a: 2})

    received = []

    Thread.new do
      sleep 0.05
      sub.close
    end

    sub.each_event do |event|
      received << event
    end

    assert_equal [{a: 1}, {a: 2}], received
  end

  def test_each_event_yields_nil_on_timeout
    sub = build
    received = []

    Thread.new do
      sleep 0.15
      sub.close
    end

    sub.each_event(timeout: 0.05) do |event|
      received << event
    end

    assert_includes received, nil, 'should yield nil at least once on idle timeout'
  end

  def test_each_event_returns_enumerator_without_block
    sub = build
    assert_kind_of Enumerator, sub.each_event
  end
end
