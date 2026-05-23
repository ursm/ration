require_relative 'ration/version'
require_relative 'ration/errors'
require_relative 'ration/configuration'
require_relative 'ration/subscription'
require_relative 'ration/hub'
require_relative 'ration/backends/base'
require_relative 'ration/backends/memory'

module Ration
  class << self
    def configure
      yield config

      if config.backend.nil?
        raise NotConfigured, 'No backend configured. Set config.backend to a Ration::Backends::* instance.'
      end

      @hub&.stop
      @hub = Hub.new(backend: config.backend, logger: config.logger)
    end

    def config
      @config ||= Configuration.new
    end

    def publish(event)
      hub.publish(event)
    end

    def subscribe(**kwargs, &block)
      hub.subscribe(**kwargs, &block)
    end

    def unsubscribe(sub)
      hub.unsubscribe(sub)
    end

    def reset!
      @hub&.stop
      @hub    = nil
      @config = nil
    end

    private

    def hub
      raise NotConfigured, 'Ration is not configured. Call Ration.configure first.' unless @hub

      @hub
    end
  end
end
