require_relative 'lib/ration/version'

Gem::Specification.new do |spec|
  spec.name        = 'ration'
  spec.version     = Ration::VERSION
  spec.authors     = ['Keita Urashima']
  spec.email       = ['ursm@ursm.jp']

  spec.summary     = 'Per-process pub/sub fan-out for Rails SSE'
  spec.description = 'A backend-agnostic event distribution layer for Rails SSE: a single listener thread per process receives events from a pub/sub backend (Postgres LISTEN/NOTIFY, Redis Pub/Sub, etc.) and fans them out to per-connection bounded queues.'
  spec.homepage    = 'https://github.com/ursm/ration'
  spec.license     = 'MIT'

  spec.required_ruby_version = '>= 3.3'

  spec.files         = Dir['lib/**/*.rb']
  spec.require_paths = ['lib']

  spec.add_dependency 'concurrent-ruby', '~> 1.2'
  spec.add_dependency 'logger',          '~> 1.6'
end
