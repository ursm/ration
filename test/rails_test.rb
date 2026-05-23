require 'test_helper'
require 'ration/rails'

class RailsSSETest < Ration::Test
  class FakeResponse
    attr_reader :headers

    def initialize
      @headers = {}
    end
  end

  class FakeRequest
    attr_reader :env, :headers

    def initialize(env = {}, headers = {})
      @env     = env
      @headers = headers
    end
  end

  class FakeController
    include Ration::Rails::SSE

    attr_accessor :response_body
    attr_reader :response, :request

    def initialize(env: {}, headers: {})
      @response = FakeResponse.new
      @request  = FakeRequest.new(env, headers)
    end
  end

  def test_sets_sse_headers
    controller = FakeController.new
    controller.sse_stream {|y| }

    assert_equal 'text/event-stream', controller.response.headers['Content-Type']
    assert_equal 'no-cache',          controller.response.headers['Cache-Control']
  end

  def test_calls_mark_as_io_bound_when_present
    called     = false
    controller = FakeController.new(env: {'puma.mark_as_io_bound' => -> { called = true }})

    controller.sse_stream {|y| }

    assert called, 'mark_as_io_bound callable should have been invoked'
  end

  def test_no_op_when_mark_as_io_bound_absent
    controller = FakeController.new

    controller.sse_stream {|y| }
  end

  def test_assigns_response_body_as_enumerator_that_invokes_block
    controller = FakeController.new

    controller.sse_stream do |y|
      y << 'one'
      y << 'two'
    end

    assert_kind_of Enumerator, controller.response_body
    assert_equal ['one', 'two'], controller.response_body.to_a
  end

  def test_passes_last_event_id_from_header_to_block
    controller = FakeController.new(headers: {'Last-Event-ID' => '42'})
    received   = nil

    controller.sse_stream {|_y, last_event_id| received = last_event_id }
    controller.response_body.to_a

    assert_equal '42', received
  end

  def test_last_event_id_is_nil_when_header_absent
    controller = FakeController.new
    received   = :unset

    controller.sse_stream {|_y, last_event_id| received = last_event_id }
    controller.response_body.to_a

    assert_nil received
  end

  def test_block_with_single_arity_still_works
    controller = FakeController.new(headers: {'Last-Event-ID' => '42'})

    controller.sse_stream do |y|
      y << 'ok'
    end

    assert_equal ['ok'], controller.response_body.to_a
  end
end
