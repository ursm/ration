require 'test_helper'
require 'ration/sse'

class SSETest < Ration::Test
  def test_string_data
    assert_equal "data: hello\n\n", Ration::SSE.event(data: 'hello')
  end

  def test_hash_data_is_json_serialized
    assert_equal %Q(data: {"a":1}\n\n), Ration::SSE.event(data: {a: 1})
  end

  def test_multi_line_string_data_is_split
    assert_equal(
      "data: line1\ndata: line2\ndata: line3\n\n",
      Ration::SSE.event(data: "line1\nline2\nline3")
    )
  end

  def test_trailing_newline_in_data_is_preserved
    assert_equal "data: hello\ndata: \n\n", Ration::SSE.event(data: "hello\n")
  end

  def test_with_id
    assert_equal "id: 42\ndata: hi\n\n", Ration::SSE.event(data: 'hi', id: 42)
  end

  def test_with_event_name
    assert_equal "event: update\ndata: hi\n\n", Ration::SSE.event(data: 'hi', event: 'update')
  end

  def test_with_retry_ms
    assert_equal "retry: 5000\ndata: hi\n\n", Ration::SSE.event(data: 'hi', retry_ms: 5000)
  end

  def test_retry_ms_zero_is_allowed
    assert_equal "retry: 0\ndata: hi\n\n", Ration::SSE.event(data: 'hi', retry_ms: 0)
  end

  def test_all_fields_field_order
    assert_equal(
      "event: update\nid: 42\nretry: 5000\ndata: hi\n\n",
      Ration::SSE.event(data: 'hi', event: 'update', id: 42, retry_ms: 5000)
    )
  end

  def test_data_nil_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: nil)
    }
  end

  def test_event_with_newline_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', event: "foo\nbar")
    }
  end

  def test_id_with_newline_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', id: "abc\ndef")
    }
  end

  def test_id_with_null_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', id: "abc\0def")
    }
  end

  def test_retry_ms_non_integer_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', retry_ms: 'fast')
    }
  end

  def test_retry_ms_negative_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', retry_ms: -1)
    }
  end

  def test_retry_ms_float_raises
    assert_raises(ArgumentError) {
      Ration::SSE.event(data: 'hi', retry_ms: 1.5)
    }
  end

  def test_comment
    assert_equal ": hi\n\n", Ration::SSE.comment('hi')
  end

  def test_empty_comment
    assert_equal ": \n\n", Ration::SSE.comment
  end

  def test_ping
    assert_equal ": ping\n\n", Ration::SSE.ping
  end

  def test_comment_with_newline_raises
    assert_raises(ArgumentError) {
      Ration::SSE.comment("hi\nthere")
    }
  end

  class FakeSubscription
    def initialize(*script)
      @script = script
      @closed = false
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end

    def each_event(timeout: nil)
      @script.each do |event|
        break if @closed

        yield event
      end

      @closed = true
    end
  end

  def test_stream_writes_framed_events_to_output
    sub    = FakeSubscription.new({id: 1, body: 'a'}, {id: 2, body: 'b'})
    output = +''

    Ration::SSE.stream(sub, output) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    assert_includes output, 'id: 1'
    assert_includes output, 'id: 2'
    assert_includes output, %Q(data: {"id":1,"body":"a"})
  end

  def test_stream_emits_ping_on_idle
    sub    = FakeSubscription.new(nil, {id: 1, body: 'a'}, nil)
    output = +''

    Ration::SSE.stream(sub, output, heartbeat: 5) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    assert_equal 2, output.scan(": ping\n\n").size
  end

  def test_stream_skips_when_block_returns_nil
    sub    = FakeSubscription.new({id: 1, drop: true}, {id: 2, keep: true})
    output = +''

    Ration::SSE.stream(sub, output, heartbeat: nil) do |event|
      next if event[:drop]

      Ration::SSE.event(data: event, id: event[:id])
    end

    refute_includes output, 'id: 1'
    assert_includes output, 'id: 2'
  end

  def test_stream_disables_heartbeat_when_nil
    sub    = FakeSubscription.new(nil, nil)
    output = +''

    Ration::SSE.stream(sub, output, heartbeat: nil) do |event|
      Ration::SSE.event(data: event)
    end

    assert_equal '', output
  end

  def test_stream_requires_block
    sub = FakeSubscription.new
    assert_raises(ArgumentError) {
      Ration::SSE.stream(sub, +'')
    }
  end

  def test_stream_since_skips_events_with_id_le_since
    sub    = FakeSubscription.new({id: 1, n: 1}, {id: 2, n: 2}, {id: 3, n: 3})
    output = +''

    Ration::SSE.stream(sub, output, heartbeat: nil, since: 2) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    refute_includes output, '"n":1'
    refute_includes output, '"n":2'
    assert_includes output, '"n":3'
  end

  def test_stream_returns_last_observed_id
    sub = FakeSubscription.new({id: 5, n: 1}, {id: 7, n: 2})

    last = Ration::SSE.stream(sub, +'', heartbeat: nil, since: 0) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    assert_equal 7, last
  end

  def test_stream_returns_since_when_no_events_pass
    sub = FakeSubscription.new({id: 1, n: 1}, {id: 2, n: 2})

    last = Ration::SSE.stream(sub, +'', heartbeat: nil, since: 10) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    assert_equal 10, last
  end

  def test_stream_returns_nil_when_since_not_given
    sub = FakeSubscription.new({id: 1, n: 1})

    last = Ration::SSE.stream(sub, +'', heartbeat: nil) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    assert_nil last
  end

  def test_stream_id_from_custom_extractor
    sub = FakeSubscription.new({uid: 1}, {uid: 2}, {uid: 3})
    output = +''

    Ration::SSE.stream(
      sub,
      output,
      heartbeat: nil,
      since:     1,
      id_from:   ->(e) { e[:uid] }
    ) do |event|
      Ration::SSE.event(data: event, id: event[:uid])
    end

    refute_includes output, '"uid":1'
    assert_includes output, '"uid":2'
    assert_includes output, '"uid":3'
  end

  def test_stream_skips_event_with_missing_id_when_since_set
    sub    = FakeSubscription.new({n: 1}, {id: 2, n: 2})
    output = +''

    Ration::SSE.stream(sub, output, heartbeat: nil, since: 0) do |event|
      Ration::SSE.event(data: event, id: event[:id])
    end

    refute_includes output, '"n":1'
    assert_includes output, '"n":2'
  end
end
