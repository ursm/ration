# Ration

Per-process pub/sub fan-out for Rails SSE.

## Why

When implementing Server-Sent Events in Rails, you need a way to push events from one place (a controller, a job, another process) into many long-lived response streams. The naive approach — having each SSE connection open its own `LISTEN` / `SUBSCRIBE` — quickly exhausts your backend connection count.

Ration solves this with a single listener thread per process. The listener holds one connection to a pub/sub backend (Postgres `LISTEN/NOTIFY`, Redis Pub/Sub, ...) and fans events out to a bounded queue per SSE connection.

```
publishers ──► [pub/sub backend] ──► [listener thread, 1 per process]
                                            │
                       ┌────────────────────┼────────────────────┐
                       ▼                    ▼                    ▼
              SizedQueue (conn A)  SizedQueue (conn B)  SizedQueue (conn C)
                       │                    │                    │
                       ▼                    ▼                    ▼
                 SSE response          SSE response          SSE response
```

Each SSE response loop does a **blocking** `queue.pop`. The listener does a **non-blocking** `queue.push`. If a slow consumer can't keep up and its queue overflows, the queue is closed — the client disconnects, reconnects, and resyncs from your persistent store via `Last-Event-ID`.

> **Heads-up on app servers.** SSE connections are long-lived. On thread-pool servers (Puma <8, Unicorn, Passenger) each connection pins a worker thread for its entire lifetime — N concurrent SSE clients require N+ workers. With **Puma 8+** the connection releases its worker via `mark_as_io_bound` (the [`Ration::Rails::SSE`](#rails-integration-rationrailssse) helper handles this for you). With **Falcon** the issue doesn't arise — each connection runs on a fiber. Beyond a handful of clients on a thread-pool server, an async server is the right choice. See [Server compatibility](#server-compatibility) for details.

## Scope

The Ration core is **transport only**. It does not:

- persist events (you keep your own table / event log)
- manage `Last-Event-ID` or resync logic
- know about SSE, HTTP, or Rails

This is deliberate, and the gem is split into independent layers so each one stays useful on its own:

```
Ration            ← core: event fan-out. Knows nothing about SSE.
Ration::SSE       ← opt-in, pure: SSE wire-format framing. Knows nothing about Rails.
Ration::Rails::SSE ← opt-in: Rails controller concern that wraps SSE headers,
                     mark_as_io_bound, and the response_body Enumerator.
```

`Ration::Rails::SSE` builds on `Ration::SSE`, which builds on `Ration`. Dependencies only flow downward, so lower layers stay reusable: the core can drive WebSockets or JSONL streams just as easily, and `Ration::SSE` works without Rails.

The intended usage is "table of truth + Ration broadcasts ids" — see [Recommended pattern](#recommended-pattern).

## Installation

```ruby
# Gemfile
gem 'ration'

# plus whichever backend you use:
gem 'pg'            # for the Postgres backend
gem 'redis-client'  # for the Redis backend
```

Requires Ruby 3.3+.

## Quick start

```ruby
# config/initializers/ration.rb
require 'ration'
require 'ration/backends/postgres'

Ration.configure do |c|
  c.backend = Ration::Backends::Postgres.new(
    url:     ENV.fetch('DATABASE_URL'),
    channel: 'app_events'
  )
  c.logger = Rails.logger
end
```

```ruby
# anywhere — controller, job, model callback
Ration.publish id: event.id, type: 'message', user_id: 42
```

```ruby
# in an SSE controller
require 'ration/sse'
require 'ration/rails'

class EventsController < ApplicationController
  include Ration::Rails::SSE

  def stream
    sse_stream {|y, last_event_id|
      Ration.subscribe(
        max:    100,
        filter: ->(e) { e[:user_id] == current_user.id }
      ) do |subscription|
        Ration::SSE.stream subscription, y, since: last_event_id&.to_i do |event|
          Ration::SSE.event(data: event, id: event[:id])
        end
      end
    }
  end
end
```

## API

### `Ration.publish(event)`

Publishes an event. `event` must be JSON-serializable. Raises `Ration::PayloadTooLarge` if the encoded size exceeds the backend's `max_payload_bytes` (default: 6 KB).

### `Ration.subscribe(max:, filter: nil, on_overflow: :close, &block)`

Creates a subscription.

| param | meaning |
| --- | --- |
| `max:` | bounded queue capacity (required) |
| `filter:` | a callable `(event) -> truthy/falsy`. Optional |
| `on_overflow:` | `:close` (default) or `:drop_oldest` |
| block | yields the `Subscription` and auto-unsubscribes on return |

If no block is given, returns the `Subscription` and the caller is responsible for calling `Ration.unsubscribe(sub)`. **The block form is strongly preferred** — it prevents registry leaks.

By the time `subscribe` returns (or the block enters), the subscription is **already buffering live events**. This is the property that makes the [recommended subscribe → backlog → drain → live loop](#recommended-pattern) ordering work.

The `Subscription` object exposes:

- `id` — a stable UUID for this subscription. Useful as a correlation key for logging and metrics.
- `pop(timeout: nil)` — blocking pop with optional timeout (seconds). Returns the next event, or `nil` on timeout or after the subscription is closed.
- `each_event(timeout: nil) { |event| ... }` — iterates until the subscription is closed. Yields events; yields `nil` on each idle timeout. With no block, returns an `Enumerator`.
- `closed?` — true if the subscription has been closed (either externally, by overflow, or by a filter exception).
- `close` — close the subscription. The consumer loop will see `nil` on its next `pop` and can detect via `closed?`.

#### Filter contract

The filter runs on the shared listener thread, on the hot path for every event published in the process. It MUST be:

- **pure** — no DB hits, no network, no shared mutable state
- **fast** — measured in microseconds
- **non-blocking** — never wait on I/O or locks

If the filter raises, Ration closes that subscription (logging the error) and other subscribers are unaffected.

```ruby
# good — pure and fast
filter: ->(e) { e[:user_id] == uid }
filter: ->(e) { e in {topic: :foo, user_id: ^uid} }
filter: -> { it in {user_id: ^uid} }                   # Ruby 3.4+

# bad — DB hit on the hot path
filter: ->(e) { User.find(e[:user_id]).subscribed_to?(e[:topic]) }
```

#### Overflow policy

- `:close` (default) — close the queue on overflow. The consumer sees `nil` from `pop`, checks `queue.closed?`, and exits. The SSE client then reconnects and resyncs from `Last-Event-ID`. This is the SSE-native model.
- `:drop_oldest` — pop the oldest event and push the new one. Useful for "latest state only" UIs (dashboards, gauges) where stale values are worse than missing intermediate ones.

#### Payload size

The default 6 KB cap is enforced by all backends, conservative against Postgres `NOTIFY`'s 8 KB hard limit. The point is to keep the same publishing code working identically regardless of which backend you wire up — Redis would happily accept megabytes, but allowing that would create a silent leaky abstraction.

The intended publish shape is small: an `id`, a type, and minimal metadata. Consumers fetch the full record from your persistent store.

## SSE framing (`Ration::SSE`)

Opt-in helper module for building SSE wire-format strings. Loaded with `require 'ration/sse'`, has no dependency on the Ration core, and has no dependency on Rails.

### `Ration::SSE.event(data:, event: nil, id: nil, retry_ms: nil)`

Returns a properly framed SSE event as a String.

- `data:` (required) — if it's a String, it's used as-is (and split across multiple `data:` fields on newlines). Otherwise `.to_json` is called.
- `id:` (optional) — sets the `id:` field. The browser will send this back as `Last-Event-ID` on reconnect, so it's the hook for resumable streams.
- `event:` (optional) — sets the `event:` field for named events (defaults to `"message"` browser-side).
- `retry_ms:` (optional) — non-negative integer milliseconds. Tells the client how long to wait before reconnecting.

Values containing newlines or NULL characters in `event:` or `id:` raise `ArgumentError`; `retry_ms:` must be a non-negative `Integer` or `ArgumentError` is raised. Multi-line strings in `data:` are correctly split into multiple `data:` fields per the SSE spec, so values like `"line1\nline2"` won't corrupt the stream.

```ruby
Ration::SSE.event(data: 'hello')
# => "data: hello\n\n"

Ration::SSE.event(data: {greeting: 'hi'}, id: 42, event: 'greeting')
# => "event: greeting\nid: 42\ndata: {\"greeting\":\"hi\"}\n\n"

Ration::SSE.event(data: "line 1\nline 2")
# => "data: line 1\ndata: line 2\n\n"

Ration::SSE.event(data: 'reconnect-tuning', retry_ms: 5000)
# => "retry: 5000\ndata: reconnect-tuning\n\n"
```

### `Ration::SSE.ping` and `Ration::SSE.comment(text)`

```ruby
Ration::SSE.ping             # => ": ping\n\n"
Ration::SSE.comment('alive') # => ": alive\n\n"
```

Comments are ignored by the EventSource client but serve as keepalives over proxies that close idle connections.

### `Ration::SSE.stream(subscription, output, heartbeat: 15, since: nil, id_from: ->(e) { e[:id] }) { |event| ... }`

Higher-level helper that joins a `Subscription` to an SSE output stream. Hides the loop, close detection, heartbeat emission, and (optionally) backlog dedup so caller code only describes how to turn one event into one SSE string.

- `subscription` — a `Ration::Subscription` (anything that responds to `each_event(timeout:)`).
- `output` — any object that responds to `<<` (a Rack `Enumerator::Yielder`, a `String`, an `IO`, ...).
- `heartbeat:` — seconds of idle before emitting a `:ping` comment. `nil` disables heartbeats.
- `since:` — events whose id is `<= since` are skipped. When `nil` (default) no dedup is performed and `id_from` is not consulted. Used to resume past a backlog you just sent.
- `id_from:` — callable that returns the id of an event. Defaults to `event[:id]`. Override for non-`Hash` events or other shapes.
- block — receives each event and returns an SSE string to append to `output`. Return `nil` (e.g. via `next`) to skip emission for an event.

Returns the highest id observed (or the original `since` if no events passed), so callers can log "last sent id" or persist progress. Returns `nil` when called without `since:`.

```ruby
last_id = Ration::SSE.stream(subscription, output, since: last_id) {|event|
  Ration::SSE.event(data: event, id: event[:id])
}
```

> Note: `since:` assumes monotonically increasing ids (e.g. a Postgres `bigserial`). Don't use it with UUIDs or other non-ordered ids.

The method returns when the subscription is closed.

## Rails integration (`Ration::Rails::SSE`)

Opt-in controller concern that bundles the boilerplate every Rails SSE endpoint shares. Loaded with `require 'ration/rails'`.

### `sse_stream { |y, last_event_id| ... }`

Sets the SSE response headers, releases the worker thread to Puma 8's I/O-bound pool if available, reads the `Last-Event-ID` request header, and assigns `response_body` to an `Enumerator` whose block produces the stream chunks.

The block receives two arguments:

- `y` — the Rack streaming yielder.
- `last_event_id` — the `Last-Event-ID` request header value as a `String`, or `nil` if absent. Pass it through to your backlog query and to [`Ration::SSE.stream`](#rationssestreamsubscription-output-heartbeat-15-since-nil-id_from-e--eid--event-)'s `since:` parameter, converting to your id type as needed (e.g. `.to_i` for integer ids).

```ruby
require 'ration/rails'

class EventsController < ApplicationController
  include Ration::Rails::SSE

  def stream
    sse_stream do |y, last_event_id|
      # use last_event_id to resume the stream
    end
  end
end
```

Equivalent to writing:

```ruby
response.headers['Content-Type']  = 'text/event-stream'
response.headers['Cache-Control'] = 'no-cache'
request.env['puma.mark_as_io_bound']&.call
last_event_id = request.headers['Last-Event-ID']
self.response_body = Enumerator.new {|y| ... yield y, last_event_id ... }
```

The `&.call` on `puma.mark_as_io_bound` makes the worker-release behavior (see [puma#3816](https://github.com/puma/puma/pull/3816)) a safe no-op on Puma <8 or other app servers — the helper works everywhere; only Puma 8+ actually releases the thread.

Ruby block arity is lenient, so callers that don't need the second argument can use `sse_stream do |y| ... end` and `last_event_id` is silently dropped.

## Recommended pattern

Ration is transport; you own the state. Combine them for resilient SSE:

```ruby
require 'ration/sse'
require 'ration/rails'

class EventsController < ApplicationController
  include Ration::Rails::SSE

  def stream
    sse_stream {|y, last_event_id|
      last_id = last_event_id.to_i

      Ration.subscribe(
        max:    100,
        filter: ->(e) { e[:user_id] == current_user.id }
      ) do |subscription|
        # 1. subscribe is done. Live events are buffering NOW.

        # 2. read the backlog from your persistent store.
        Event.where(user_id: current_user.id)
             .where('id > ?', last_id)
             .find_each do |evt|
          y << Ration::SSE.event(data: evt, id: evt.id)
          last_id = evt.id
        end

        # 3. live loop. Loop, close detection, heartbeat, and backlog dedup
        #    are handled by stream; the block only does framing.
        Ration::SSE.stream subscription, y, since: last_id do |event|
          Ration::SSE.event(data: event, id: event[:id])
        end
      end
    }
  end
end
```

The ordering matters:

1. **Subscribe first** so live events buffer in your queue.
2. **Read the backlog** from your persistent store, using `Last-Event-ID`.
3. **Drain the queue** with id-based dedup against what you just sent.

Reading the backlog before subscribing would drop any events arriving in between.

> The example uses Rack streaming via the [`Ration::Rails::SSE`](#rails-integration-rationrailssse) helper, which handles headers, `mark_as_io_bound`, and the `response_body` Enumerator. `ActionController::Live` is intentionally out of scope — it has known rough edges around exceptions and thread cleanup.

## Backends

### Memory

In-process pub/sub. For tests and single-process scripts.

```ruby
Ration::Backends::Memory.new(
  max_payload_bytes: 6 * 1024,  # default
  sync:              false       # set true to deliver inline on publish
)
```

`sync: true` makes `publish` deliver to all subscribers on the calling thread — convenient for deterministic tests.

### Postgres (`LISTEN`/`NOTIFY`)

```ruby
Ration::Backends::Postgres.new(
  url:               ENV.fetch('DATABASE_URL'),
  channel:           'ration',
  max_payload_bytes: 6 * 1024,
  poll_interval:     1.0,
  publish_with:      nil,
  logger:            Rails.logger
)
```

- The listener holds one dedicated connection. `start` connects and issues `LISTEN` **synchronously**; if the first connection fails, `start` raises. Reconnection after that is automatic with exponential backoff (1s → 30s cap).
- `poll_interval:` controls how often the listener wakes to check for shutdown. **Does not affect delivery latency** — `LISTEN/NOTIFY` is push-driven, so events arrive on the listener thread the moment they're published. The poll only governs how quickly `stop()` is observed; the default 1.0s is fine.
- `publish_with:` lets you publish through your existing connection pool instead of opening a fresh PG connection per `publish`. **Strongly recommended in production:**

  ```ruby
  publish_with: ->(channel, payload) {
    ActiveRecord::Base.connection_pool.with_connection do |c|
      c.raw_connection.exec_params('SELECT pg_notify($1, $2)', [channel, payload])
    end
  }
  ```

No migration needed — `LISTEN/NOTIFY` is built into Postgres. There's no events table.

### Redis (Pub/Sub)

```ruby
Ration::Backends::Redis.new(
  url:               ENV.fetch('REDIS_URL'),
  channel:           'ration',
  max_payload_bytes: 6 * 1024,
  poll_interval:     1.0,
  publish_with:      nil,
  logger:            Rails.logger
)
```

Same shape as the Postgres backend, using `redis-client`. The 6 KB cap is enforced for cross-backend consistency, not because Redis requires it. `poll_interval:` has the same meaning as in the Postgres backend — shutdown-wake only, not delivery latency.

## Per-process semantics

The listener thread is **per process**. In a typical Puma deployment, that means N listener threads across N workers — each worker maintains exactly one backend connection regardless of how many SSE connections it serves. This is the design's whole point.

## Server compatibility

Ration itself is server-agnostic, but SSE connections are long-lived and how they consume server resources depends on the app server:

- **Async / fiber-based servers (Falcon, etc.)** — handle this naturally. Each SSE connection runs on a fiber, doesn't pin an OS thread, and you can hold many thousands of connections per process. No special configuration needed.
- **Puma 8+** — the [`Ration::Rails::SSE`](#rails-integration-rationrailssse) helper invokes `request.env['puma.mark_as_io_bound']&.call` for you (see [puma#3816](https://github.com/puma/puma/pull/3816)), releasing the worker thread back to the pool while the connection blocks on event delivery.
- **Puma <8, Unicorn, Passenger, and other thread/process-pool servers without I/O-bound support** — each SSE connection occupies a worker thread or process for its entire lifetime. Size your pool with this in mind: N concurrent SSE clients require N+ workers plus headroom for regular requests. Beyond a handful of concurrent SSE clients, an async server is a much better fit.

## Development

```sh
bundle install

# core tests + Memory backend tests
bundle exec rake

# with Postgres tests
RATION_TEST_DATABASE_URL=postgres:///postgres bundle exec rake

# with Redis tests
RATION_TEST_REDIS_URL=redis://localhost:6379 bundle exec rake

# everything
RATION_TEST_DATABASE_URL=postgres:///postgres \
  RATION_TEST_REDIS_URL=redis://localhost:6379 \
  bundle exec rake
```

CI runs all three against Postgres and Redis service containers (see `.github/workflows/test.yml`).

## License

MIT.
