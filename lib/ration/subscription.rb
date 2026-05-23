require 'securerandom'

module Ration
  class Subscription
    OVERFLOW_POLICIES = %i[close drop_oldest].freeze

    attr_reader :id

    def initialize(max:, filter: nil, on_overflow: :close, logger:)
      unless OVERFLOW_POLICIES.include?(on_overflow)
        raise ArgumentError, "Unknown on_overflow: #{on_overflow.inspect} (expected one of #{OVERFLOW_POLICIES.inspect})"
      end

      @id          = SecureRandom.uuid
      @queue       = SizedQueue.new(max)
      @filter      = filter
      @on_overflow = on_overflow
      @logger      = logger
    end

    def pop(timeout: nil)
      @queue.pop(timeout: timeout)
    end

    def each_event(timeout: nil)
      return enum_for(:each_event, timeout: timeout) unless block_given?

      until closed?
        event = pop(timeout: timeout)
        break if closed?

        yield event
      end

      self
    end

    def closed?
      @queue.closed?
    end

    def close
      @queue.close
    end

    def deliver(event)
      return if closed?
      return unless passes_filter?(event)

      begin
        @queue.push(event, true)
      rescue ThreadError
        handle_overflow(event)
      rescue ClosedQueueError
        # closed concurrently; nothing to do
      end
    end

    private

    def passes_filter?(event)
      return true if @filter.nil?

      @filter.call(event)
    rescue => e
      @logger.error("Ration filter raised, closing subscription #{@id}: #{e.class}: #{e.message}")
      close
      false
    end

    def handle_overflow(event)
      case @on_overflow
      when :close
        close
      when :drop_oldest
        begin
          @queue.pop(true)
          @queue.push(event, true)
        rescue ThreadError
          # race: another thread popped first, or queue got closed; ignore
        rescue ClosedQueueError
          # closed concurrently; ignore
        end
      end
    end
  end
end
