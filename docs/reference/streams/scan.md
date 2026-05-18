---
id: scan
title: "Scan"
---

`zio.blocks.streams.scan.Scan[-In, +Out]` is a description of a **stateful
transducer** that threads input `In` to output `Out` while accumulating a
typed `State` value. It complements [`Pipeline`](./pipeline.md) (stateless
element transformations) and [`Sink`](./sink.md) (single-summary
consumption) by making composable, multi-summary pipelines first-class.

The `State` type is hidden as a type member and refined via
`Scan.Aux[-In, +Out, S0]` when you need it precise — mirroring the
`ToStructural.Aux[A, S]` / `UnapplySeq.Aux[X, C0[_], A0]` conventions
elsewhere in `zio-blocks`. Most user code uses `Scan[In, Out]` and
factories return `Scan.Aux` so composition has the precise state in scope
automatically.

## When to use `Scan` vs `Pipeline` / `Sink`

Reach for `Scan` when at least one of the following is true:

1. You need **more than one stateful summary out of one pass** — e.g.
   compute a hash AND a byte count AND running sample-statistics from a
   single drain. Use `&&&` to fan out; the state collapses to a flat
   tuple via `combinators.Tuples`.
2. You need **resumption** — pause the pipeline, persist the typed final
   state, and resume later from where you left off. Use
   `scan.withInitialState(savedState)`.
3. You're building a **chain of three or more dependent stateful stages**
   — e.g. the MACD example below. The `>>>` chain reads more clearly
   than equivalent stream piping plus a custom `Sink.create`.

For everything else, stay on `Pipeline` / `Sink` — they're simpler. You
can always escape into them via `scan.toPipeline` and `scan.toSink`.

## Basic usage

```scala
import zio.blocks.chunk.Chunk
import zio.blocks.streams.{Stream, _}
import zio.blocks.streams.scan._

// One stateful scan: count + running fold, both surfaced.
val scan: Scan.Aux[Int, Int, (Long, Int)] =
  Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)

val state: Either[Nothing, (Long, Int)] =
  Stream(1, 2, 3, 4).run(scan.toSink)   // Right((4L, 10))
```

## Composition

| Operator       | Meaning                                                                                                | State merge       |
|----------------|--------------------------------------------------------------------------------------------------------|-------------------|
| `a >>> b`      | Sequential: feed `a`'s output into `b`                                                                 | via `Combine`     |
| `a &&& b`      | Fanout: send every input to both, pair the outputs                                                     | via `Combine`     |
| `s.map(f)`     | Transform every output                                                                                 | unchanged         |
| `s.contramap(f)` | Pre-process every input                                                                              | unchanged         |
| `s.dimap(g)(f)` | `contramap(g) >>> map(f)`                                                                             | unchanged         |
| `s.mapState(f)` | Project the state                                                                                     | `S2`              |

`Combine` is a thin priority ladder that disambiguates `Tuples[Unit, Unit]`
(both `leftUnit` and `rightUnit` from upstream `Tuples` would otherwise
match) and otherwise delegates to `combinators.Tuples` so you keep its
Scala 3 auto-flattening:

- `Unit *.* Unit` => `Unit`            — for stateless+stateless fanout
- `Unit *.* B`    => `B`                — stateless+stateful collapses
- `A *.* Unit`    => `A`
- otherwise       => `Tuples`-flattened tuple (`(A, B, C, D, …)` flat)

## Resumption (`withInitialState`)

Every `Scan` carries `withInitialState(s: State): Scan.Aux[In, Out, State]`
so a published final state can seed a new run. Composed scans split the
saved state via `Tuples.separate` and recursively re-initialise each
child; stateless scans short-circuit to `this`.

```scala
val scan      = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
val midState  = Stream(1, 2, 3).run(scan.toSink).toOption.get
val resumed   = scan.withInitialState(midState)
val finalSt   = Stream(4, 5).run(resumed.toSink).toOption.get
// finalSt == Stream(1, 2, 3, 4, 5).run(scan.toSink).toOption.get
```

State serialisation is left to the user — `zio.blocks.schema.Schema`,
JSON, Avro, or anything else.

## Short-circuit

All short-circuit scans use a `private var doneSent: Boolean = false`
flag and call `source.close()` when the limit/predicate is hit — **no
exceptions** for control flow. Mirrors `Reader.TakenWhile` exactly.

| Factory                            | Behaviour                                              | State          |
|------------------------------------|--------------------------------------------------------|----------------|
| `Scan.take(n)`                     | Pass through at most `n` elements                       | count emitted  |
| `Scan.takeWhile(pred)`             | Pass through while `pred` holds                         | count emitted  |
| `Scan.haltWhen(pred)`              | Stop on the first matching element (excluded)           | count emitted  |
| `Scan.drop(n)`                     | Skip the first `n` elements                             | count dropped  |
| `Scan.dropWhile(pred)`             | Skip the longest matching prefix                        | count dropped  |

## Scope-aware completion

```scala
// Run a finalizer exactly once on close (success / error / short-circuit)
val safe = Scan.ensuring(Scan.identity[Int]) { closeFile() }

// Acquire / release a resource around the scan
val ingest = Scan.acquireRelease(openFile(), (f: File) => f.close()) { _ =>
  Scan.count[Byte]
}
// State: (File, Long) -- the resource paired with the inner state
```

Both run on top of the existing `Reader` / `DelegatingReader` /
`Stream.fromAcquireRelease` machinery — no new lifecycle plumbing.

## Statistical scans

| Factory                            | Behaviour                                                          |
|------------------------------------|--------------------------------------------------------------------|
| `Scan.count[A]`                    | pass through; state = `Long` (count)                                |
| `Scan.zipWithIndex[A]`             | pair each element with its 0-based index                            |
| `Scan.fold(z)(f)`                  | left fold; emit nothing; state = final fold value                   |
| `Scan.runningFold(z)(f)`           | left fold; emit running fold value per element                      |
| `Scan.pairwise[A]`                 | emit `(prev, curr)` from element 2 onward                            |
| `Scan.diff[A: Numeric]`            | emit `curr - prev` from element 2 onward                             |
| `Scan.ewma(alpha)`                 | exponentially-weighted moving average                                |
| `Scan.sampleStats`                 | `Double => SampleStats` running stats; state = current `SampleStats` |
| `Scan.sampleStatsTerminal`         | consume silently; state = final `SampleStats`                        |
| `Scan.sampleStatsFromInitial(s)`   | resume `sampleStats` from a saved value                              |

`SampleStats` (Welford for `observe`, Chan parallel algorithm for
`merge`) tracks size + first four central moments, and exposes
`mean`, `variance`, `stddev`, `skewness`, `kurtosis` (sample +
population variants). Forms a Monoid under `merge`.

## Time / windowing

| Factory                           | Behaviour                                                                |
|-----------------------------------|--------------------------------------------------------------------------|
| `Scan.timestamped(clock)`         | wrap each element as `Timestamped(nanos, value)`                          |
| `Scan.tumbling(size)`             | count-based tumbling windows                                              |
| `Scan.tumblingTime(durationNs)`   | event-time tumbling windows over `Timestamped[A]`                         |
| `Scan.sliding(size, step)`        | count-based sliding windows; final partial emitted                        |

## MACD over 10-minute returns

```scala
import zio.blocks.streams.scan._

val tenMinNs = 10L * 60_000_000_000L

val macd =
  Scan.tumblingTime[Double](tenMinNs) >>>
  Scan.lift[Chunk[Timestamped[Double]], Double] { c =>
    var s = 0.0; var i = 0
    while (i < c.length) { s += c(i).value; i += 1 }
    s / c.length.toDouble
  } >>>
  Scan.pairwise[Double] >>>
  Scan.lift[(Double, Double), Double] { case (p, c) => c - p } >>>
  Scan.ewma(0.2)
// Aux[Timestamped[Double], Double, (Long, Double)]

stream.via(macd.toPipeline)   // Pipeline emitting smoothed MACD signal
stream.run(macd.toSink)       // returns (#windows, final EWMA value)
```

## Hashing (JVM)

`zio.blocks.streams.scan.HashScan` (in `streams/jvm/...`) provides
chunk-aware byte hashing backed by `java.security.MessageDigest`:

```scala
import zio.blocks.streams.scan._

val payload: Stream[Nothing, Chunk[Byte]] = ...

// Pass-through: emits each chunk; final digest surfaces as state.
val digest =
  payload.run(HashScan.digest(HashScan.HashAlgo.SHA256).toSink[Nothing])

// Or fan out: digest AND chunk count in one pass:
val (digest2, chunkCount) =
  payload.run((HashScan.digest(HashScan.HashAlgo.SHA256) &&& Scan.count[Chunk[Byte]])
                .toSink[Nothing]).toOption.get
```

Each chunk is fed to `MessageDigest.update(arr, off, len)` in **a
single JNI call** — no per-byte loop. The state surfaces as
`Chunk[Byte]` (the digest bytes); fan-out via `&&&` collapses to
`(Chunk[Byte], Long)` via `Tuples`.

For users with a `Stream[E, Byte]` instead of `Stream[E, Chunk[Byte]]`,
batch bytes into chunks first via `Stream.grouped(size)` before piping
into a hash scan.

## Materialisation

| Method            | Result                                            |
|-------------------|---------------------------------------------------|
| `scan.toPipeline` | `Pipeline[In, Out]` — drops the state             |
| `scan.toSink[E]`  | `Sink[E, In, State]` — surfaces the final state   |
| `scan.runChunk(in)` | `(State, Chunk[Out])` — for tests              |

## Limitations

- No effects in scan steps. If you need effects, wrap with `Stream.eval` outside the scan.
- `HashScan` is JVM-only (uses `java.security.MessageDigest`).
- Single-fiber semantics. `&&&` is a logical fanout in one fiber, not a multi-fiber broadcast.
- Element-level pushback (general `flatMap` with leftover hand-off for non-byte streams) is a v2 feature.
