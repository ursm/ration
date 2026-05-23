$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))

require 'minitest/autorun'
require 'logger'
require 'ration'

module Ration
  class Test < Minitest::Test
    def setup
      Ration.reset!
    end

    def teardown
      Ration.reset!
    end

    def silent_logger
      Logger.new(nil)
    end
  end
end
