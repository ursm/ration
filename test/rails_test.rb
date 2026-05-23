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
    attr_reader :env

    def initialize(env = {})
      @env = env
    end
  end

  class FakeController
    include Ration::Rails::SSE

    attr_accessor :response_body
    attr_reader :response, :request

    def initialize(env = {})
      @response = FakeResponse.new
      @request  = FakeRequest.new(env)
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
    controller = FakeController.new('puma.mark_as_io_bound' => -> { called = true })

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
end
