require 'ration'
require 'ration/sse'

module Ration
  module Rails
    module SSE
      def sse_stream(&block)
        response.headers['Content-Type']  = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        request.env['puma.mark_as_io_bound']&.call

        last_event_id = request.headers['Last-Event-ID']

        self.response_body = Enumerator.new {|y| block.call(y, last_event_id) }
      end
    end
  end
end
