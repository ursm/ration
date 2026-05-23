require 'concurrent'

module Ration
  class Hub
    def initialize(backend:, logger:)
      @backend       = backend
      @logger        = logger
      @subscriptions = Concurrent::Map.new
      @started       = false
      @start_mutex   = Mutex.new
    end

    def publish(event)
      ensure_started
      @backend.publish(event)
    end

    def subscribe(max:, filter: nil, on_overflow: :close)
      ensure_started

      sub = Subscription.new(
        max:         max,
        filter:      filter,
        on_overflow: on_overflow,
        logger:      @logger
      )
      @subscriptions[sub.id] = sub

      return sub unless block_given?

      begin
        yield sub
      ensure
        unsubscribe(sub)
      end
    end

    def unsubscribe(sub)
      @subscriptions.delete(sub.id)
      sub.close
    end

    def stop
      @start_mutex.synchronize do
        @subscriptions.each_value(&:close)
        @subscriptions.clear
        @backend.stop if @started
        @started = false
      end
    end

    def subscription_count
      @subscriptions.size
    end

    private

    def ensure_started
      return if @started

      @start_mutex.synchronize do
        return if @started

        @backend.on_event {|event| deliver(event) }
        @backend.start
        @started = true
      end
    end

    def deliver(event)
      closed_ids = []

      @subscriptions.each_pair do |id, sub|
        begin
          sub.deliver(event)
        rescue => e
          @logger.error("Ration delivery error for subscription #{id}: #{e.class}: #{e.message}")
        end

        closed_ids << id if sub.closed?
      end

      closed_ids.each {|id| @subscriptions.delete(id) }
    end
  end
end
