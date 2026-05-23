require 'test_helper'

class MemoryBackendTest < Ration::Test
  def test_sync_mode_emits_immediately
    backend  = Ration::Backends::Memory.new(sync: true)
    received = []
    backend.on_event {|e| received << e }
    backend.start

    backend.publish({n: 1})
    backend.publish({n: 2})

    assert_equal [{n: 1}, {n: 2}], received
  end

  def test_async_mode_emits_via_listener_thread
    backend  = Ration::Backends::Memory.new
    received = Queue.new
    backend.on_event {|e| received.push(e) }
    backend.start

    begin
      backend.publish({n: 1})
      backend.publish({n: 2})

      assert_equal({n: 1}, received.pop)
      assert_equal({n: 2}, received.pop)
    ensure
      backend.stop
    end
  end

  def test_payload_too_large
    backend = Ration::Backends::Memory.new(max_payload_bytes: 100, sync: true)
    backend.start

    assert_raises(Ration::PayloadTooLarge) {
      backend.publish({data: 'x' * 200})
    }
  end

  def test_top_level_api
    Ration.configure do |c|
      c.backend = Ration::Backends::Memory.new(sync: true)
      c.logger  = silent_logger
    end

    received = []

    Ration.subscribe(max: 10) do |subscription|
      Ration.publish({n: 1})
      Ration.publish({n: 2})

      while (event = subscription.pop(timeout: 0))
        received << event
      end
    end

    assert_equal [{n: 1}, {n: 2}], received
  end

  def test_publish_without_configure_raises
    assert_raises(Ration::NotConfigured) {
      Ration.publish({n: 1})
    }
  end
end
