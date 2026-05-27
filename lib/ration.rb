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

      backend = config.backend or
        raise NotConfigured, 'No backend configured. Set config.backend to a Ration::Backends::* instance.'

      @hub&.stop
      @hub = Hub.new(backend: backend, logger: config.logger)
    end

    def config
      @config ||= Configuration.new
    end

    def publish(event)
      hub.publish(event)
    end

    def subscribe(...)
      hub.subscribe(...) # steep:ignore UnresolvedOverloading
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
      @hub or raise NotConfigured, 'Ration is not configured. Call Ration.configure first.'
    end
  end
end
