require 'logger'

module Ration
  class Configuration
    attr_accessor :backend, :logger

    def initialize
      @backend = nil
      @logger  = Logger.new($stderr)
    end
  end
end
