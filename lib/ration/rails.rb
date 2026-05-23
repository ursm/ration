require 'ration'
require 'ration/sse'

module Ration
  module Rails
    module SSE
      def sse_stream(&block)
        response.headers['Content-Type']  = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        request.env['puma.mark_as_io_bound']&.call

        self.response_body = Enumerator.new(&block)
      end
    end
  end
end
