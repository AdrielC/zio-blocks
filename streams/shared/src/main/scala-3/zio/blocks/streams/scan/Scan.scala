/*
 * Copyright 2024-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.blocks.streams.scan

import scala.annotation.unchecked.uncheckedVariance
import zio.blocks.chunk.{Chunk, ChunkBuilder}
import zio.blocks.streams.{JvmType, Pipeline, Sink, Stream}
import zio.blocks.streams.internal.EndOfStream
import zio.blocks.streams.io.Reader

/**
 * A description of a stateful transducer with typed inputs, outputs and a typed
 * state value that evolves as scans compose.
 *
 * The `State` member is hidden as a type member and refined via [[Scan.Aux]]
 * when the user wants it precise (mirroring the upstream
 * `ToStructural.Aux[A, S]` and `UnapplySeq.Aux[X, C0[_], A0]` conventions).
 * Most user code uses `Scan[In, Out]` and never names the state explicitly;
 * factories return [[Scan.Aux]] so composition has the precise state in scope
 * automatically.
 *
 * Composition (`>>>`, `&&&`) normalises the state type via
 * [[zio.blocks.combinators.Tuples]]. `Unit` is the identity, so stateless scans
 * never widen the state; two stateful scans yield a flat tuple, with deeper
 * nesting auto-flattened on Scala 3 and macro-flattened on Scala 2.
 *
 * Internal mutable state lives as `private var`s inside the `ScanReader`
 * instances each scan produces — the user never sees it.
 *
 * @tparam In
 *   Element input type (contravariant).
 * @tparam Out
 *   Element output type (covariant).
 *
 * @example
 *   {{{
 *   import zio.blocks.streams.{Stream, _}
 *   import zio.blocks.streams.scan.Scan
 *
 *   val scan: Scan.Aux[Int, Int, Long] =
 *     Scan.lift[Int, Int](_ * 2) >>> Scan.count
 *
 *   Stream(1, 2, 3, 4).run(scan.toSink)  // Right(4L)
 *   }}}
 */
abstract class Scan[-In, +Out] { self =>

  /** The state / summary type. Refinable via [[Scan.Aux]]. */
  type State

  /** The state value before any input has been consumed. */
  def initialState: State

  /**
   * Reconstruct this scan with `s` as the starting state.
   *
   * Composed scans (`AndThen`, `Both`, …) split the saved state via
   * `Tuples.separate` and re-initialise each child independently. Stateless
   * scans return `this` unchanged.
   *
   * Use case: persist a final state from one run via [[toSink]], then resume a
   * later run from that state by passing it to `withInitialState`.
   */
  def withInitialState(s: State): Scan.Aux[In, Out, State]

  /** Human-readable rendering of this scan's structure. */
  def render: String

  /** Final override so all subclasses get a sane `toString`. */
  override final def toString: String = render

  /**
   * Build a [[ScanReader]] whose `.state` is exactly `self.State`. Internal —
   * users go through [[toPipeline]] / [[toSink]].
   */
  private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = self.State }

  // ---------------------------------------------------------------------------
  //  Sequential composition
  // ---------------------------------------------------------------------------

  /**
   * Sequential composition: feed the output of `this` into `that`. The state
   * type merges via [[Combine]] (`Unit` is the identity, so stateless+stateful
   * stays single-state).
   */
  final def >>>[Out2, S2](that: Scan.Aux[Out, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[In, Out2, t.Out] =
    new Scan.AndThen[In, Out, Out2, State, S2, t.Out](self, that, t)

  /** Alias for [[>>>]]. */
  final def andThen[Out2, S2](that: Scan.Aux[Out, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[In, Out2, t.Out] = self >>> that

  // ---------------------------------------------------------------------------
  //  Fanout: same input, paired output
  // ---------------------------------------------------------------------------

  /**
   * Fanout / broadcast: send every input to both `this` and `that`, returning a
   * paired output. State types are merged via
   * [[zio.blocks.combinators.Tuples]].
   *
   * Implementation note: a single source is read once and each pulled element
   * is fed to a one-slot mailbox per child Reader. There is no actual
   * concurrency — this is a logical fanout in one fiber.
   */
  final def &&&[Out2, S2](that: Scan.Aux[In @uncheckedVariance, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[In, (Out, Out2), t.Out] =
    new Scan.Both[In, Out, Out2, State, S2, t.Out](self, that, t)

  // ---------------------------------------------------------------------------
  //  Arrow.Choice: Either-input ops (parity with fs2 `Scan.choice`/`choose`)
  // ---------------------------------------------------------------------------

  /**
   * Choice (`+++`): feed `Left` inputs through `this`, `Right` inputs through
   * `that`; both branches must emit the same output type. The state is
   * merged via `Combine`.
   *
   * Matches the Arrow.Choice combinator and fs2's `Scan#choice`.
   */
  final def +++[In2, Out2 >: Out, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[Either[In, In2], Out2, t.Out] =
    new Scan.Choice[In, In2, Out2, State, S2, t.Out](
      self.asInstanceOf[Scan.Aux[In, Out2, State]],
      that,
      t
    )

  /** Alias for [[+++]]. */
  final def choice[In2, Out2 >: Out, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[Either[In, In2], Out2, t.Out] = self +++ that

  /**
   * Choose (`|||`): feed `Left` inputs through `this`, `Right` inputs through
   * `that`; outputs are wrapped in their own `Either`. State merged via
   * `Combine`.
   *
   * Matches fs2's `Scan#choose`.
   */
  final def |||[In2, Out2, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[Either[In, In2], Either[Out, Out2], t.Out] =
    new Scan.Choose[In, In2, Out, Out2, State, S2, t.Out](self, that, t)

  /** Alias for [[|||]]. */
  final def choose[In2, Out2, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[Either[In, In2], Either[Out, Out2], t.Out] = self ||| that

  // ---------------------------------------------------------------------------
  //  Arrow split: pair-input, paired output
  // ---------------------------------------------------------------------------

  /**
   * Split (`***`): apply `this` to the left half of a pair-input and `that`
   * to the right half. State merged via `Combine`.
   *
   * Matches the canonical Arrow `***` (a.k.a. `split`).
   */
  final def ***[In2, Out2, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[(In, In2), (Out, Out2), t.Out] =
    new Scan.Parallel[In, In2, Out, Out2, State, S2, t.Out](self, that, t)

  /** Alias for [[***]]. */
  final def split[In2, Out2, S2](that: Scan.Aux[In2, Out2, S2])(using
    t: Combine[State, S2]
  ): Scan.Aux[(In, In2), (Out, Out2), t.Out] = self *** that

  // ---------------------------------------------------------------------------
  //  Strong arrow: tuple/either lensing
  // ---------------------------------------------------------------------------

  /** Lensed input/output: `In = (X, A)` => `Out = (Y, A)`. State unchanged. */
  final def first[A]: Scan.Aux[(In, A), (Out, A), State] =
    new Scan.First[In, A, Out, State](self)

  /** Lensed input/output: `In = (A, X)` => `Out = (A, Y)`. State unchanged. */
  final def second[A]: Scan.Aux[(A, In), (A, Out), State] =
    new Scan.Second[In, A, Out, State](self)

  /**
   * Generic lensing: extract `In` from `I2`, run the scan, recombine output
   * with the original `I2`. State unchanged.
   *
   * Matches fs2's `Scan#lens`. The state never widens — `lens` is a pure
   * input/output lens through the scan's internal state.
   */
  final def lens[I2, O2](get: I2 => In, set: (I2, Out) => O2): Scan.Aux[I2, O2, State] =
    new Scan.Lensed[In, I2, Out, O2, State](self, get, set)

  /**
   * Semilens: like [[lens]] but `extract` may short-circuit by returning
   * `Left(o2)` — those values bypass the scan entirely. Matches fs2's
   * `Scan#semilens`.
   */
  final def semilens[I2, O2](
    extract: I2 => Either[O2, In],
    inject: (I2, Out) => O2
  ): Scan.Aux[I2, O2, State] =
    new Scan.Semilensed[In, I2, Out, O2, State](self, extract, inject)

  /**
   * Like [[semilens]] but the original scan's output passes straight
   * through. Matches fs2's `Scan#semipass`.
   */
  final def semipass[I2, O2 >: Out](extract: I2 => Either[O2, In]): Scan.Aux[I2, O2, State] =
    semilens(extract, (_, o) => o)

  /**
   * Left lens over an `Either` input: `Left` values flow through the scan;
   * `Right` values pass through unchanged. Matches fs2's `Scan#left`.
   */
  final def left[A]: Scan.Aux[Either[In, A], Either[Out, A], State] =
    semilens(
      {
        case Left(i)  => Right(i)
        case Right(a) => Left(Right(a))
      },
      (_, o) => Left(o)
    )

  /**
   * Right lens over an `Either` input. Matches fs2's `Scan#right`.
   */
  final def right[A]: Scan.Aux[Either[A, In], Either[A, Out], State] =
    semilens(
      {
        case Right(i) => Right(i)
        case Left(a)  => Left(Left(a))
      },
      (_, o) => Right(o)
    )

  // ---------------------------------------------------------------------------
  //  Single-element step (parity with fs2 `Scan#step`)
  // ---------------------------------------------------------------------------

  /**
   * Apply this scan to a single input, returning a new scan whose state is
   * advanced by one element and the chunk of outputs produced.
   *
   * Parity with fs2's `Scan#step`. Convenient for testing and for stepping
   * a scan from the outside (e.g. interpreting a chunk one element at a
   * time while inspecting the intermediate states).
   */
  final def step(i: In): (Scan.Aux[In, Out, State], Chunk[Out]) = {
    val (state, out) = self.runChunk(Chunk.single(i.asInstanceOf[In @uncheckedVariance]))
    (
      self.withInitialState(state).asInstanceOf[Scan.Aux[In, Out, State]],
      out.asInstanceOf[Chunk[Out]]
    )
  }

  // ---------------------------------------------------------------------------
  //  Output / input transforms (don't change State)
  // ---------------------------------------------------------------------------

  /** Transform every output element through `f`. */
  final def map[Out2](f: Out => Out2): Scan.Aux[In, Out2, State] =
    new Scan.MappedOut[In, Out, Out2, State](self, f)

  /** Pre-process every input element through `f`. */
  final def contramap[In2](f: In2 => In): Scan.Aux[In2, Out, State] =
    new Scan.MappedIn[In, In2, Out, State](self, f)

  /**
   * Compose a contravariant input transform and a covariant output transform.
   */
  final def dimap[In2, Out2](g: In2 => In)(f: Out => Out2): Scan.Aux[In2, Out2, State] =
    contramap(g).map(f)

  // ---------------------------------------------------------------------------
  //  State projection
  // ---------------------------------------------------------------------------

  /**
   * Project the state through `f` (doesn't change inputs/outputs).
   *
   * On Scala 3 this is `inline` and short-circuits to `self` when `S =:= S2`
   * (so e.g. `s.mapState(identity)` and `s.mapState[State](Predef.identity)`
   * are zero-cost). Mirrors `Sink.mapError` / `Stream.mapError`.
   */
  inline final def mapState[S2](inline f: State => S2): Scan.Aux[In, Out, S2] =
    scala.compiletime.summonFrom {
      case _: (State =:= S2) => self.asInstanceOf[Scan.Aux[In, Out, S2]]
      case _                 => new Scan.MappedState[In, Out, State, S2](self, f)
    }

  /**
   * Re-ascribe the state type when you statically know the equivalence. Useful
   * for adapting to a structural refinement at the API boundary.
   */
  final def asAux[S2](using ev: State =:= S2): Scan.Aux[In, Out, S2] =
    self.asInstanceOf[Scan.Aux[In, Out, S2]]

  // ---------------------------------------------------------------------------
  //  Materialisation
  // ---------------------------------------------------------------------------

  /**
   * Drop the state and produce a [[Pipeline]]. Use with `Stream.via(…)`.
   *
   * Requires the input and output types to have a [[JvmType.Infer]] in scope so
   * the resulting `Stream` keeps its primitive lane (no boxing).
   */
  final def toPipeline(using
    jtIn: JvmType.Infer[In @uncheckedVariance],
    jtOut: JvmType.Infer[Out @uncheckedVariance]
  ): Pipeline[In, Out] = new Scan.AsPipeline[In, Out, State](self, jtIn, jtOut)

  /**
   * A [[Sink]] that drains its input and returns the final state.
   *
   * Path-dependent on `self.State`. To name the state explicitly at the call
   * site, type the receiver as `Scan.Aux[In, Out, S]`.
   */
  final def toSink[E]: Sink[E, In, State] = new Scan.AsSink[E, In, Out, State](self)

  /** Run the scan against an in-memory chunk. For tests only. */
  final def runChunk(in: Chunk[In]): (State, Chunk[Out]) =
    Scan.runChunkImpl[In, Out, State](self.asInstanceOf[Scan.Aux[In, Out, State]], in)
}

/**
 * Companion for [[Scan]]. Provides the [[Aux]] refinement type alias and a
 * library of factory constructors.
 *
 * Factories return [[Scan.Aux]] so the precise `State` is in scope at the call
 * site. User code that doesn't care about state simply uses `Scan[In, Out]`.
 */
object Scan {

  /**
   * Refinement type alias matching `ToStructural.Aux[A, S]` /
   * `UnapplySeq.Aux[X, C0[_], A0]` style: trait parameters first (`In`, `Out`),
   * refined type member last (`S0`).
   */
  type Aux[-In, +Out, S0] = Scan[In, Out] { type State = S0 }

  // ---------------------------------------------------------------------------
  //  Stateless leaf factories
  // ---------------------------------------------------------------------------

  /** The identity scan. Passes every input through unchanged. */
  def identity[A]: Scan.Aux[A, A, Unit] = new Identity[A]

  /** A scan that maps every input through `f`. State is `Unit`. */
  def lift[I, O](f: I => O): Scan.Aux[I, O, Unit] = new Lift[I, O](f)

  /**
   * A scan that emits zero or many outputs per input via `f`. State is `Unit`.
   */
  def emit[I, O](f: I => Chunk[O]): Scan.Aux[I, O, Unit] = new Emit[I, O](f)

  /** A scan that drops elements not matching `pred`. State is `Unit`. */
  def filter[A](pred: A => Boolean): Scan.Aux[A, A, Unit] = new FilterScan[A](pred)

  /** A scan that emits only elements where `pf` is defined. State is `Unit`. */
  def collect[A, B](pf: PartialFunction[A, B]): Scan.Aux[A, B, Unit] = new CollectScan[A, B](pf)

  // ---------------------------------------------------------------------------
  //  Stateful leaf factories
  // ---------------------------------------------------------------------------

  /**
   * A scan that passes every input through and counts them. State is `Long`.
   */
  def count[A]: Scan.Aux[A, A, Long] = new Counted[A]

  /** A scan that pairs every input with its 0-based index. State is `Long`. */
  def zipWithIndex[A]: Scan.Aux[A, (Long, A), Long] = new ZipWithIndex[A]

  /**
   * A scan that consumes silently and surfaces only the final fold value as its
   * state. Equivalent to `Sink.foldLeft(z)(f)` but composes with other scans
   * via `>>>` / `&&&`.
   */
  def fold[I, S](z: S)(f: (S, I) => S): Scan.Aux[I, Nothing, S] = new FoldLeftScan[I, S](z, f)

  /**
   * A scan that emits the running fold value per element. State is the current
   * fold value.
   */
  def runningFold[I, S](z: S)(f: (S, I) => S): Scan.Aux[I, S, S] =
    new RunningFoldScan[I, S](z, f)

  // ---------------------------------------------------------------------------
  //  Short-circuit
  // ---------------------------------------------------------------------------

  /**
   * Pass through at most `n` elements, then close the upstream and surface the
   * count of emitted elements as the state.
   *
   * Implemented via a `private var doneSent: Boolean` flag — no exceptions —
   * mirroring `Reader.TakenWhile` exactly.
   */
  def take[A](n: Long): Scan.Aux[A, A, Long] = new TakeN[A](n)

  /**
   * Pass elements through while `pred` holds; on the first failing element,
   * close upstream and surface the count of emitted elements.
   */
  def takeWhile[A](pred: A => Boolean): Scan.Aux[A, A, Long] = new TakeWhileScan[A](pred)

  /**
   * Pass elements through until (but not including) the first element for which
   * `pred` returns `true`; close upstream and surface the count of emitted
   * elements.
   */
  def haltWhen[A](pred: A => Boolean): Scan.Aux[A, A, Long] = new HaltWhenScan[A](pred)

  /**
   * Skip the first `n` elements; surface the actual number dropped as state.
   */
  def drop[A](n: Long): Scan.Aux[A, A, Long] = new DropN[A](n)

  /**
   * Skip the longest prefix of elements for which `pred` returns `true`, then
   * pass the rest through; surface the count of dropped elements as state.
   */
  def dropWhile[A](pred: A => Boolean): Scan.Aux[A, A, Long] = new DropWhileScan[A](pred)

  // ---------------------------------------------------------------------------
  //  Scope-aware completion
  // ---------------------------------------------------------------------------

  /**
   * Run `finalizer` exactly once when this scan's reader closes — whether
   * through end-of-stream, error, or short-circuit. State is unchanged.
   *
   * Mirrors upstream's `Reader.DelegatingReader` finalisation pattern.
   */
  def ensuring[In, Out, S](inner: Scan.Aux[In, Out, S])(finalizer: => Unit): Scan.Aux[In, Out, S] =
    new Ensuring[In, Out, S](inner, () => finalizer)

  /**
   * Acquire a `Resource[R]` lazily on first read, thread it into `use`, and
   * release it on close. Built on the existing `Stream.fromAcquireRelease` /
   * `Reader.DelegatingReader` machinery; the resulting state is the pair of the
   * acquired resource and the inner scan's state.
   */
  def acquireRelease[In, Out, S, R](
    acquire: => R,
    release: R => Unit
  )(use: R => Scan.Aux[In, Out, S]): Scan.Aux[In, Out, (R, S)] =
    new Scoped[In, Out, S, R](() => acquire, release, use)

  // ---------------------------------------------------------------------------
  //  Time / windowing
  // ---------------------------------------------------------------------------

  /**
   * Tag every input with a nanosecond timestamp from `clock` (default
   * `System.nanoTime`). State is `Unit`. Used as the input adapter for
   * [[tumblingTime]].
   */
  def timestamped[A](clock: () => Long = () => System.nanoTime()): Scan.Aux[A, Timestamped[A], Unit] =
    new TimestampedScan[A](clock)

  /**
   * Group elements into count-based tumbling windows of size `size`. Each
   * window is emitted as a `Chunk[A]`; the final, possibly-shorter window is
   * emitted at end-of-stream. State is the number of windows emitted.
   */
  def tumbling[A](size: Int): Scan.Aux[A, Chunk[A], Long] = new TumblingScan[A](size)

  /**
   * Group `Timestamped[A]` elements by event-time tumbling windows of
   * `durationNanos`. The first window starts at the timestamp of the first
   * element; subsequent windows are computed from there. State is the number of
   * windows emitted (the final partial window is emitted on close).
   */
  def tumblingTime[A](durationNanos: Long): Scan.Aux[Timestamped[A], Chunk[Timestamped[A]], Long] =
    new TumblingTimeScan[A](durationNanos)

  /**
   * Sliding count-based windows of size `size` advancing by `step`. State is
   * the number of windows emitted.
   */
  def sliding[A](size: Int, step: Int = 1): Scan.Aux[A, Chunk[A], Long] = new SlidingScan[A](size, step)

  // ---------------------------------------------------------------------------
  //  Stats
  // ---------------------------------------------------------------------------

  /** Emit `(prev, curr)` from the second input onward. Stateless. */
  def pairwise[A]: Scan.Aux[A, (A, A), Unit] = new PairwiseScan[A]

  /** Emit `curr - prev` from the second input onward. */
  def diff[A](implicit num: Numeric[A]): Scan.Aux[A, A, Unit] =
    pairwise[A].map { case (p, c) => num.minus(c, p) }

  /**
   * Exponentially-weighted moving average:
   * `e_t = alpha * x_t + (1 - alpha) * e_{t-1}`, seeded by the first
   * observation. State is the current EWMA value. Forgetful — old values
   * weighted less than recent ones.
   */
  def ewma(alpha: Double): Scan.Aux[Double, Double, Double] = new EwmaScan(alpha)

  /**
   * Online sample statistics over a stream of `Double`s. Emits the running
   * `SampleStats` per observation; the final state is the same.
   *
   * Uses Welford's algorithm for `observe` and the Chan parallel algorithm for
   * `merge` — together a numerically stable Monoid.
   */
  def sampleStats: Scan.Aux[Double, SampleStats, SampleStats] = new SampleStatsRunning(SampleStats.empty)

  /**
   * Like [[sampleStats]] but consumes silently and surfaces only the final
   * stats via `toSink` / `state`.
   */
  def sampleStatsTerminal: Scan.Aux[Double, Nothing, SampleStats] = new SampleStatsTerminal(SampleStats.empty)

  /** Resume a running [[sampleStats]] from a saved value. */
  def sampleStatsFromInitial(initial: SampleStats): Scan.Aux[Double, SampleStats, SampleStats] =
    new SampleStatsRunning(initial)

  /** Run a scan in-memory. Used by [[Scan.runChunk]] and tests. */
  private[scan] def runChunkImpl[In, Out, S](scan: Scan.Aux[In, Out, S], in: Chunk[In]): (S, Chunk[Out]) = {
    val src     = Reader.fromChunk(in)(using JvmType.Infer.anyRef[In]).asInstanceOf[Reader[In]]
    val out     = scan.applyToReader(src)
    val builder = ChunkBuilder.make[Out](16)
    var v       = out.read[Any](EndOfStream)
    while (v.asInstanceOf[AnyRef] ne EndOfStream) {
      builder += v.asInstanceOf[Out]
      v = out.read[Any](EndOfStream)
    }
    val st = out.state
    out.close()
    (st, builder.result())
  }

  // ===========================================================================
  //  Concrete leaf classes
  // ===========================================================================

  /**
   * Identity scan. Pulls one element per call and forwards it. Stateless.
   *
   * Forwards `jvmType` from the source so primitive-lane specialisation is
   * preserved.
   */
  private[scan] final class Identity[A] extends Scan[A, A] {
    type State = Unit
    def initialState: Unit                                                                  = ()
    def withInitialState(s: Unit): Scan.Aux[A, A, Unit]                                     = this
    def render: String                                                                      = "Scan.identity"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Unit } =
      new ScanReader[A] {
        type State = Unit
        def state: Unit                     = ()
        override def jvmType: JvmType       = source.jvmType
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: A](sentinel: A1): A1 = source.read[A1](sentinel)
        def close(): Unit                   = source.close()
      }
  }

  /** A stateless 1:1 transform. */
  private[scan] final class Lift[I, O](f: I => O) extends Scan[I, O] {
    type State = Unit
    def initialState: Unit                                                                  = ()
    def withInitialState(s: Unit): Scan.Aux[I, O, Unit]                                     = this
    def render: String                                                                      = "Scan.lift(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[O] { type State = Unit } =
      new ScanReader[O] {
        type State = Unit
        def state: Unit                     = ()
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: O](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else f(v.asInstanceOf[I]).asInstanceOf[A1]
        }
        def close(): Unit = source.close()
      }
  }

  /**
   * A stateless 1:N transform. The output chunk is buffered and drained one
   * element per call so [[Reader.read]] semantics stay one-element-at-a-time.
   */
  private[scan] final class Emit[I, O](f: I => Chunk[O]) extends Scan[I, O] {
    type State = Unit
    def initialState: Unit                                                                  = ()
    def withInitialState(s: Unit): Scan.Aux[I, O, Unit]                                     = this
    def render: String                                                                      = "Scan.emit(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[O] { type State = Unit } =
      new ScanReader[O] {
        type State = Unit
        def state: Unit                     = ()
        private var pending: Chunk[O]       = Chunk.empty
        private var pendingIx: Int          = 0
        def isClosed: Boolean               = source.isClosed && pendingIx >= pending.length
        def read[A1 >: O](sentinel: A1): A1 = {
          while (true) {
            if (pendingIx < pending.length) {
              val o = pending(pendingIx); pendingIx += 1
              return o.asInstanceOf[A1]
            }
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            pending = f(v.asInstanceOf[I])
            pendingIx = 0
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  /** A stateless filter. */
  private[scan] final class FilterScan[A](pred: A => Boolean) extends Scan[A, A] {
    type State = Unit
    def initialState: Unit                                                                  = ()
    def withInitialState(s: Unit): Scan.Aux[A, A, Unit]                                     = this
    def render: String                                                                      = "Scan.filter(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Unit } =
      new ScanReader[A] {
        type State = Unit
        def state: Unit                     = ()
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: A](sentinel: A1): A1 = {
          while (true) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            val a = v.asInstanceOf[A]
            if (pred(a)) return a.asInstanceOf[A1]
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  /** A stateless partial-function collect. */
  private[scan] final class CollectScan[A, B](pf: PartialFunction[A, B]) extends Scan[A, B] {
    type State = Unit
    def initialState: Unit                                                                  = ()
    def withInitialState(s: Unit): Scan.Aux[A, B, Unit]                                     = this
    def render: String                                                                      = "Scan.collect(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[B] { type State = Unit } =
      new ScanReader[B] {
        type State = Unit
        def state: Unit                     = ()
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: B](sentinel: A1): A1 = {
          while (true) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            val a = v.asInstanceOf[A]
            if (pf.isDefinedAt(a)) return pf(a).asInstanceOf[A1]
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  /** A scan that passes elements through and increments a Long counter. */
  private[scan] final class Counted[A] private (initial: Long) extends Scan[A, A] {
    def this() = this(0L)
    type State = Long
    def initialState: Long                                                                  = initial
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new Counted[A](s)
    def render: String                                                                      = "Scan.count"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var n: Long                 = initial
        def state: Long                     = n
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: A](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else { n += 1L; v.asInstanceOf[A1] }
        }
        def close(): Unit = source.close()
      }
  }

  /** Pairs every input with its 0-based index. State is the next index. */
  private[scan] final class ZipWithIndex[A] private (initial: Long) extends Scan[A, (Long, A)] {
    def this() = this(0L)
    type State = Long
    def initialState: Long                                                                          = initial
    def withInitialState(s: Long): Scan.Aux[A, (Long, A), Long]                                     = new ZipWithIndex[A](s)
    def render: String                                                                              = "Scan.zipWithIndex"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[(Long, A)] { type State = Long } =
      new ScanReader[(Long, A)] {
        type State = Long
        private var i: Long                         = initial
        def state: Long                             = i
        def isClosed: Boolean                       = source.isClosed
        def read[A1 >: (Long, A)](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            val tuple = (i, v.asInstanceOf[A])
            i += 1L
            tuple.asInstanceOf[A1]
          }
        }
        def close(): Unit = source.close()
      }
  }

  /**
   * Pure left fold; emits no elements per step, surfaces the final fold through
   * `toSink` / `state`.
   */
  private[scan] final class FoldLeftScan[I, S](z: S, f: (S, I) => S) extends Scan[I, Nothing] {
    type State = S
    def initialState: S                                                                        = z
    def withInitialState(s: S): Scan.Aux[I, Nothing, S]                                        = new FoldLeftScan[I, S](s, f)
    def render: String                                                                         = "Scan.fold(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[Nothing] { type State = S } =
      new ScanReader[Nothing] {
        type State = S
        private var acc: S                        = z
        def state: S                              = acc
        def isClosed: Boolean                     = source.isClosed
        def read[A1 >: Nothing](sentinel: A1): A1 = {
          // Emits nothing per element; drain the source eagerly here so that
          // a `.read(sentinel)` call advances the fold and returns sentinel
          // exactly once at end-of-stream.
          var v = source.read[Any](EndOfStream)
          while (v.asInstanceOf[AnyRef] ne EndOfStream) {
            acc = f(acc, v.asInstanceOf[I])
            v = source.read[Any](EndOfStream)
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  /** Emits the running fold value per element. */
  private[scan] final class RunningFoldScan[I, S](z: S, f: (S, I) => S) extends Scan[I, S] {
    type State = S
    def initialState: S                                                                  = z
    def withInitialState(s: S): Scan.Aux[I, S, S]                                        = new RunningFoldScan[I, S](s, f)
    def render: String                                                                   = "Scan.runningFold(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[S] { type State = S } =
      new ScanReader[S] {
        type State = S
        private var acc: S                  = z
        def state: S                        = acc
        def isClosed: Boolean               = source.isClosed
        def read[A1 >: S](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else { acc = f(acc, v.asInstanceOf[I]); acc.asInstanceOf[A1] }
        }
        def close(): Unit = source.close()
      }
  }

  // ===========================================================================
  //  Short-circuit leaf classes — flag-based, no exceptions.
  // ===========================================================================

  /**
   * Pass through at most `n` elements, then close upstream. State is the count
   * of emitted elements. Resumption: `withInitialState(emitted)` starts a new
   * scan that emits up to `n` more (independent of the saved count), but
   * reports `saved + emittedSoFar` as state.
   */
  private[scan] final class TakeN[A] private (limit: Long, initialEmitted: Long) extends Scan[A, A] {
    def this(limit: Long) = this(limit, 0L)
    type State = Long
    def initialState: Long                                                                  = initialEmitted
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new TakeN[A](limit, s)
    def render: String                                                                      = s"Scan.take($limit)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var emitted: Long           = 0L
        private var doneSent: Boolean       = false
        def state: Long                     = initialEmitted + emitted
        def isClosed: Boolean               = doneSent || source.isClosed
        override def jvmType: JvmType       = source.jvmType
        def read[A1 >: A](sentinel: A1): A1 =
          if (doneSent) sentinel
          else if (emitted >= limit) {
            doneSent = true
            source.close()
            sentinel
          } else {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) { doneSent = true; sentinel }
            else { emitted += 1L; v.asInstanceOf[A1] }
          }
        def close(): Unit = { doneSent = true; source.close() }
      }
  }

  private[scan] final class TakeWhileScan[A] private (pred: A => Boolean, initialEmitted: Long) extends Scan[A, A] {
    def this(pred: A => Boolean) = this(pred, 0L)
    type State = Long
    def initialState: Long                                                                  = initialEmitted
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new TakeWhileScan[A](pred, s)
    def render: String                                                                      = "Scan.takeWhile(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var emitted: Long           = 0L
        private var doneSent: Boolean       = false
        def state: Long                     = initialEmitted + emitted
        def isClosed: Boolean               = doneSent || source.isClosed
        override def jvmType: JvmType       = source.jvmType
        def read[A1 >: A](sentinel: A1): A1 = {
          if (doneSent) return sentinel
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) { doneSent = true; sentinel }
          else {
            val a = v.asInstanceOf[A]
            if (pred(a)) { emitted += 1L; a.asInstanceOf[A1] }
            else { doneSent = true; source.close(); sentinel }
          }
        }
        def close(): Unit = { doneSent = true; source.close() }
      }
  }

  private[scan] final class HaltWhenScan[A] private (pred: A => Boolean, initialEmitted: Long) extends Scan[A, A] {
    def this(pred: A => Boolean) = this(pred, 0L)
    type State = Long
    def initialState: Long                                                                  = initialEmitted
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new HaltWhenScan[A](pred, s)
    def render: String                                                                      = "Scan.haltWhen(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var emitted: Long           = 0L
        private var doneSent: Boolean       = false
        def state: Long                     = initialEmitted + emitted
        def isClosed: Boolean               = doneSent || source.isClosed
        override def jvmType: JvmType       = source.jvmType
        def read[A1 >: A](sentinel: A1): A1 = {
          if (doneSent) return sentinel
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) { doneSent = true; sentinel }
          else {
            val a = v.asInstanceOf[A]
            if (pred(a)) { doneSent = true; source.close(); sentinel }
            else { emitted += 1L; a.asInstanceOf[A1] }
          }
        }
        def close(): Unit = { doneSent = true; source.close() }
      }
  }

  private[scan] final class DropN[A] private (n: Long, initialDropped: Long) extends Scan[A, A] {
    def this(n: Long) = this(n, 0L)
    type State = Long
    def initialState: Long                                                                  = initialDropped
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new DropN[A](n, s)
    def render: String                                                                      = s"Scan.drop($n)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var dropped: Long           = 0L
        def state: Long                     = initialDropped + dropped
        def isClosed: Boolean               = source.isClosed
        override def jvmType: JvmType       = source.jvmType
        def read[A1 >: A](sentinel: A1): A1 = {
          while (dropped < n) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            dropped += 1L
          }
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel else v.asInstanceOf[A1]
        }
        def close(): Unit = source.close()
      }
  }

  private[scan] final class DropWhileScan[A] private (pred: A => Boolean, initialDropped: Long) extends Scan[A, A] {
    def this(pred: A => Boolean) = this(pred, 0L)
    type State = Long
    def initialState: Long                                                                  = initialDropped
    def withInitialState(s: Long): Scan.Aux[A, A, Long]                                     = new DropWhileScan[A](pred, s)
    def render: String                                                                      = "Scan.dropWhile(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State = Long
        private var dropped: Long           = 0L
        private var dropping: Boolean       = true
        def state: Long                     = initialDropped + dropped
        def isClosed: Boolean               = source.isClosed
        override def jvmType: JvmType       = source.jvmType
        def read[A1 >: A](sentinel: A1): A1 = {
          while (true) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            val a = v.asInstanceOf[A]
            if (dropping) {
              if (pred(a)) { dropped += 1L }
              else { dropping = false; return a.asInstanceOf[A1] }
            } else return a.asInstanceOf[A1]
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  // ===========================================================================
  //  Scope-aware leaf classes
  // ===========================================================================

  /** Run `finalizer` exactly once when the scan's reader closes. */
  private[scan] final class Ensuring[In, Out, S0](inner: Scan.Aux[In, Out, S0], finalizer: () => Unit)
      extends Scan[In, Out] {
    type State = S0
    def initialState: S0                                                                     = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In, Out, S0]                                       = new Ensuring[In, Out, S0](inner.withInitialState(s), finalizer)
    def render: String                                                                       = s"${inner.render}.ensuring(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = S0 } = {
      val rd = inner.applyToReader(source)
      new ScanReader[Out] {
        type State = S0
        private var ranFinalizer: Boolean     = false
        def state: S0                         = rd.state
        override def jvmType: JvmType         = rd.jvmType
        def isClosed: Boolean                 = rd.isClosed
        def read[A1 >: Out](sentinel: A1): A1 = rd.read[A1](sentinel)
        def close(): Unit                     = {
          var primary: Throwable = null
          try rd.close()
          catch { case t: Throwable => primary = t }
          if (!ranFinalizer) {
            ranFinalizer = true
            try finalizer()
            catch {
              case t: Throwable =>
                if (primary == null) primary = t
                else primary.addSuppressed(t)
            }
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  /**
   * Acquire `R` lazily on first reader build, thread it through `use`, and
   * release on close.
   */
  private[scan] final class Scoped[In, Out, S, R](
    acquire: () => R,
    release: R => Unit,
    use: R => Scan.Aux[In, Out, S]
  ) extends Scan[In, Out] {
    type State = (R, S)
    def initialState: (R, S) = {
      // Resumption with pre-built state would require constructing a Scan
      // from a saved `R` — but we don't know how to re-acquire one here.
      // Initial state defers to `use(acquire())` lazily; we build it here
      // to satisfy the API contract, matching `Stream.fromAcquireRelease`
      // semantics where `acquire` may run on every materialisation.
      val r = acquire()
      (r, use(r).initialState)
    }
    def withInitialState(s: (R, S)): Scan.Aux[In, Out, (R, S)] = {
      val (r, sInner) = s
      new Scoped[In, Out, S, R](() => r, release, r0 => use(r0).withInitialState(sInner))
    }
    def render: String                                                                           = "Scan.acquireRelease(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = (R, S) } = {
      val r  = acquire()
      val rd =
        try use(r).applyToReader(source)
        catch {
          case t: Throwable =>
            try release(r)
            catch { case _: Throwable => () }
            throw t
        }
      new ScanReader[Out] {
        type State = (R, S)
        private var released: Boolean         = false
        def state: (R, S)                     = (r, rd.state)
        override def jvmType: JvmType         = rd.jvmType
        def isClosed: Boolean                 = rd.isClosed
        def read[A1 >: Out](sentinel: A1): A1 = rd.read[A1](sentinel)
        def close(): Unit                     = {
          var primary: Throwable = null
          try rd.close()
          catch { case t: Throwable => primary = t }
          if (!released) {
            released = true
            try release(r)
            catch {
              case t: Throwable =>
                if (primary == null) primary = t
                else primary.addSuppressed(t)
            }
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  // ===========================================================================
  //  Time / window leaf classes
  // ===========================================================================

  private[scan] final class TimestampedScan[A](clock: () => Long) extends Scan[A, Timestamped[A]] {
    type State = Unit
    def initialState: Unit                                                                               = ()
    def withInitialState(s: Unit): Scan.Aux[A, Timestamped[A], Unit]                                     = this
    def render: String                                                                                   = "Scan.timestamped(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[Timestamped[A]] { type State = Unit } =
      new ScanReader[Timestamped[A]] {
        type State = Unit
        def state: Unit                                  = ()
        def isClosed: Boolean                            = source.isClosed
        def read[A1 >: Timestamped[A]](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else Timestamped(clock(), v.asInstanceOf[A]).asInstanceOf[A1]
        }
        def close(): Unit = source.close()
      }
  }

  /**
   * Count-based tumbling windows. Buffers up to `size` elements then emits a
   * `Chunk[A]`. Final, possibly-shorter window emitted at end-of-stream.
   */
  private[scan] final class TumblingScan[A] private (size: Int, initialEmittedWindows: Long) extends Scan[A, Chunk[A]] {
    def this(size: Int) = this(size, 0L)
    require(size > 0, s"TumblingScan size must be > 0, got $size")
    type State = Long
    def initialState: Long                                                                         = initialEmittedWindows
    def withInitialState(s: Long): Scan.Aux[A, Chunk[A], Long]                                     = new TumblingScan[A](size, s)
    def render: String                                                                             = s"Scan.tumbling($size)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[Chunk[A]] { type State = Long } =
      new ScanReader[Chunk[A]] {
        type State = Long
        private var buf: ChunkBuilder[A]           = ChunkBuilder.make[A](size)
        private var bufLen: Int                    = 0
        private var emittedSinceStart: Long        = 0L
        private var sourceDone: Boolean            = false
        private var done: Boolean                  = false
        def state: Long                            = initialEmittedWindows + emittedSinceStart
        def isClosed: Boolean                      = done
        def read[A1 >: Chunk[A]](sentinel: A1): A1 = {
          if (done) return sentinel
          if (!sourceDone) {
            while (bufLen < size) {
              val v = source.read[Any](EndOfStream)
              if (v.asInstanceOf[AnyRef] eq EndOfStream) {
                sourceDone = true
                if (bufLen == 0) { done = true; return sentinel }
                val chunk = buf.result()
                buf = ChunkBuilder.make[A](size)
                bufLen = 0
                emittedSinceStart += 1L
                done = true // no more elements after this final partial window
                return chunk.asInstanceOf[A1]
              }
              buf += v.asInstanceOf[A]
              bufLen += 1
            }
            val chunk = buf.result()
            buf = ChunkBuilder.make[A](size)
            bufLen = 0
            emittedSinceStart += 1L
            chunk.asInstanceOf[A1]
          } else {
            done = true
            sentinel
          }
        }
        def close(): Unit = { done = true; source.close() }
      }
  }

  /**
   * Event-time tumbling windows over `Timestamped[A]`. The first window starts
   * at the timestamp of the first element observed.
   */
  private[scan] final class TumblingTimeScan[A] private (
    durationNanos: Long,
    initialEmittedWindows: Long
  ) extends Scan[Timestamped[A], Chunk[Timestamped[A]]] {
    def this(durationNanos: Long) = this(durationNanos, 0L)
    require(durationNanos > 0L, s"TumblingTimeScan durationNanos must be > 0, got $durationNanos")
    type State = Long
    def initialState: Long                                                               = initialEmittedWindows
    def withInitialState(s: Long): Scan.Aux[Timestamped[A], Chunk[Timestamped[A]], Long] =
      new TumblingTimeScan[A](durationNanos, s)
    def render: String = s"Scan.tumblingTime($durationNanos)"
    private[scan] def applyToReader(
      source: Reader[Timestamped[A]]
    ): ScanReader[Chunk[Timestamped[A]]] { type State = Long } =
      new ScanReader[Chunk[Timestamped[A]]] {
        type State = Long
        private var buf: ChunkBuilder[Timestamped[A]] = ChunkBuilder.make(16)
        private var bufLen: Int                       = 0
        private var emittedSinceStart: Long           = 0L
        private var windowStart: Long                 = 0L
        private var initialised: Boolean              = false
        private var pending: Timestamped[A]           = null
        private var sourceDone: Boolean               = false
        private var done: Boolean                     = false
        def state: Long                               = initialEmittedWindows + emittedSinceStart
        def isClosed: Boolean                         = done
        private def emitBuf(): Chunk[Timestamped[A]]  = {
          val chunk = buf.result()
          buf = ChunkBuilder.make(16)
          bufLen = 0
          emittedSinceStart += 1L
          chunk
        }
        def read[A1 >: Chunk[Timestamped[A]]](sentinel: A1): A1 = {
          if (done) return sentinel
          while (true) {
            // re-process any pending element first (it crossed a window boundary
            // and was held back from the previous read)
            val candidate: Timestamped[A] =
              if (pending != null) {
                val p = pending; pending = null; p
              } else if (sourceDone) {
                if (bufLen > 0) return emitBuf().asInstanceOf[A1]
                done = true
                return sentinel
              } else {
                val v = source.read[Any](EndOfStream)
                if (v.asInstanceOf[AnyRef] eq EndOfStream) {
                  sourceDone = true
                  if (bufLen > 0) return emitBuf().asInstanceOf[A1]
                  done = true
                  return sentinel
                }
                v.asInstanceOf[Timestamped[A]]
              }
            if (!initialised) {
              windowStart = candidate.nanos
              initialised = true
            }
            if (candidate.nanos < windowStart + durationNanos) {
              buf += candidate
              bufLen += 1
            } else {
              // boundary — emit current buffer, advance window start, hold this element
              pending = candidate
              // skip empty windows so we don't emit a glut of empty chunks
              while (candidate.nanos >= windowStart + durationNanos) windowStart += durationNanos
              if (bufLen > 0) return emitBuf().asInstanceOf[A1]
              // empty buffer — fall through to process pending in next iteration
            }
          }
          sentinel
        }
        def close(): Unit = { done = true; source.close() }
      }
  }

  /** Count-based sliding windows of size `size` advancing by `step`. */
  private[scan] final class SlidingScan[A] private (size: Int, step: Int, initialEmittedWindows: Long)
      extends Scan[A, Chunk[A]] {
    def this(size: Int, step: Int) = this(size, step, 0L)
    require(size > 0, s"SlidingScan size must be > 0, got $size")
    require(step > 0, s"SlidingScan step must be > 0, got $step")
    type State = Long
    def initialState: Long                                                                         = initialEmittedWindows
    def withInitialState(s: Long): Scan.Aux[A, Chunk[A], Long]                                     = new SlidingScan[A](size, step, s)
    def render: String                                                                             = s"Scan.sliding($size, $step)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[Chunk[A]] { type State = Long } =
      new ScanReader[Chunk[A]] {
        type State = Long
        private val ring                    = scala.collection.mutable.ArrayBuffer.empty[A]
        private var emittedSinceStart: Long = 0L
        private var firstEmitted: Boolean   = false
        private var sourceDone: Boolean     = false
        private var done: Boolean           = false
        def state: Long                     = initialEmittedWindows + emittedSinceStart
        def isClosed: Boolean               = done
        private def buildChunk(): Chunk[A]  = {
          val arr = new scala.Array[Any](ring.length)
          var i   = 0
          while (i < ring.length) { arr(i) = ring(i); i += 1 }
          // Use the AnyRef chunk builder to avoid primitive specialisation
          // concerns for now — Phase 1B-or-later may add per-lane fast paths.
          val cb = ChunkBuilder.make[A](ring.length)
          var j  = 0
          while (j < ring.length) { cb += arr(j).asInstanceOf[A]; j += 1 }
          cb.result()
        }
        private def fill(target: Int): Boolean = {
          while (ring.length < target) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) { sourceDone = true; return false }
            ring.append(v.asInstanceOf[A])
          }
          true
        }
        def read[A1 >: Chunk[A]](sentinel: A1): A1 = {
          if (done) return sentinel
          if (!firstEmitted) {
            if (!fill(size)) {
              if (ring.isEmpty) { done = true; return sentinel }
              firstEmitted = true
              val chunk = buildChunk()
              ring.clear()
              emittedSinceStart += 1L
              done = true
              return chunk.asInstanceOf[A1]
            }
            firstEmitted = true
            val chunk = buildChunk()
            emittedSinceStart += 1L
            return chunk.asInstanceOf[A1]
          }
          // advance by step elements, then emit if we still have a full window
          var i = 0
          while (i < step) {
            if (ring.nonEmpty) ring.remove(0)
            i += 1
          }
          if (sourceDone && ring.isEmpty) { done = true; return sentinel }
          if (!fill(size)) {
            if (ring.isEmpty) { done = true; return sentinel }
            val chunk = buildChunk()
            ring.clear()
            emittedSinceStart += 1L
            done = true
            return chunk.asInstanceOf[A1]
          }
          val chunk = buildChunk()
          emittedSinceStart += 1L
          chunk.asInstanceOf[A1]
        }
        def close(): Unit = { done = true; source.close() }
      }
  }

  // ===========================================================================
  //  Stat leaf classes
  // ===========================================================================

  private[scan] final class PairwiseScan[A] extends Scan[A, (A, A)] {
    type State = Unit
    def initialState: Unit                                                                       = ()
    def withInitialState(s: Unit): Scan.Aux[A, (A, A), Unit]                                     = this
    def render: String                                                                           = "Scan.pairwise"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[(A, A)] { type State = Unit } =
      new ScanReader[(A, A)] {
        type State = Unit
        private var prev: A                      = null.asInstanceOf[A]
        private var hasPrev: Boolean             = false
        def state: Unit                          = ()
        def isClosed: Boolean                    = source.isClosed
        def read[A1 >: (A, A)](sentinel: A1): A1 = {
          while (true) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            val a = v.asInstanceOf[A]
            if (!hasPrev) {
              prev = a
              hasPrev = true
            } else {
              val pair = (prev, a)
              prev = a
              return pair.asInstanceOf[A1]
            }
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  /** Exponentially-weighted moving average. */
  private[scan] final class EwmaScan(alpha: Double, initial: Double, hasFirstInitial: Boolean)
      extends Scan[Double, Double] {
    def this(alpha: Double) = this(alpha, 0.0, false)
    require(alpha > 0.0 && alpha <= 1.0, s"ewma alpha must be in (0, 1], got $alpha")
    type State = Double
    def initialState: Double                                          = initial
    def withInitialState(s: Double): Scan.Aux[Double, Double, Double] =
      new EwmaScan(alpha, s, true)
    def render: String                                                                                  = s"Scan.ewma($alpha)"
    private[scan] def applyToReader(source: Reader[Double]): ScanReader[Double] { type State = Double } =
      new ScanReader[Double] {
        type State = Double
        private var ewma: Double                 = initial
        private var hasFirst: Boolean            = hasFirstInitial
        def state: Double                        = ewma
        def isClosed: Boolean                    = source.isClosed
        def read[A1 >: Double](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            val x = v.asInstanceOf[Double]
            if (!hasFirst) {
              ewma = x
              hasFirst = true
            } else {
              ewma = alpha * x + (1.0 - alpha) * ewma
            }
            ewma.asInstanceOf[A1]
          }
        }
        def close(): Unit = source.close()
      }
  }

  /** Running [[SampleStats]] — emits per observation. */
  private[scan] final class SampleStatsRunning(initial: SampleStats) extends Scan[Double, SampleStats] {
    type State = SampleStats
    def initialState: SampleStats                                                    = initial
    def withInitialState(s: SampleStats): Scan.Aux[Double, SampleStats, SampleStats] =
      new SampleStatsRunning(s)
    def render: String                                                                                            = "Scan.sampleStats"
    private[scan] def applyToReader(source: Reader[Double]): ScanReader[SampleStats] { type State = SampleStats } =
      new ScanReader[SampleStats] {
        type State = SampleStats
        private var stats: SampleStats                = initial
        def state: SampleStats                        = stats
        def isClosed: Boolean                         = source.isClosed
        def read[A1 >: SampleStats](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            stats = stats.observe(v.asInstanceOf[Double])
            stats.asInstanceOf[A1]
          }
        }
        def close(): Unit = source.close()
      }
  }

  /**
   * Terminal [[SampleStats]] — consumes silently, surfaces only the final
   * value.
   */
  private[scan] final class SampleStatsTerminal(initial: SampleStats) extends Scan[Double, Nothing] {
    type State = SampleStats
    def initialState: SampleStats                                                = initial
    def withInitialState(s: SampleStats): Scan.Aux[Double, Nothing, SampleStats] =
      new SampleStatsTerminal(s)
    def render: String                                                                                        = "Scan.sampleStatsTerminal"
    private[scan] def applyToReader(source: Reader[Double]): ScanReader[Nothing] { type State = SampleStats } =
      new ScanReader[Nothing] {
        type State = SampleStats
        private var stats: SampleStats            = initial
        def state: SampleStats                    = stats
        def isClosed: Boolean                     = source.isClosed
        def read[A1 >: Nothing](sentinel: A1): A1 = {
          var v = source.read[Any](EndOfStream)
          while (v.asInstanceOf[AnyRef] ne EndOfStream) {
            stats = stats.observe(v.asInstanceOf[Double])
            v = source.read[Any](EndOfStream)
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  // ===========================================================================
  //  Composition wrappers
  // ===========================================================================

  /**
   * Sequential composition: `first` then `second`. State merges via
   * [[zio.blocks.combinators.Tuples]].
   */
  private[scan] final class AndThen[In, Mid, Out, SA, SB, S0](
    val fst: Scan.Aux[In, Mid, SA],
    val snd: Scan.Aux[Mid, Out, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[In, Out] {
    type State = S0
    def initialState: S0                               = t.combine(fst.initialState, snd.initialState)
    def withInitialState(s: S0): Scan.Aux[In, Out, S0] = {
      val (a, b) = t.separate(s)
      new AndThen[In, Mid, Out, SA, SB, S0](
        fst.withInitialState(a),
        snd.withInitialState(b),
        t
      )
    }
    def render: String                                                                       = s"${fst.render} >>> ${snd.render}"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = S0 } = {
      val mid: ScanReader[Mid] { type State = SA } = fst.applyToReader(source)
      val out: ScanReader[Out] { type State = SB } = snd.applyToReader(mid)
      new ScanReader[Out] {
        type State = S0
        def state: S0                         = t.combine(mid.state, out.state)
        override def jvmType: JvmType         = out.jvmType
        def isClosed: Boolean                 = out.isClosed
        def read[A1 >: Out](sentinel: A1): A1 = out.read[A1](sentinel)
        def close(): Unit                     = out.close()
      }
    }
  }

  /**
   * Fanout / broadcast: feed every input to both `left` and `right`. The source
   * is pulled once per pair; each pulled element is mailbox-fed to both child
   * Readers.
   */
  private[scan] final class Both[In, OA, OB, SA, SB, S0](
    val lhs: Scan.Aux[In, OA, SA],
    val rhs: Scan.Aux[In, OB, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[In, (OA, OB)] {
    type State = S0
    def initialState: S0                                    = t.combine(lhs.initialState, rhs.initialState)
    def withInitialState(s: S0): Scan.Aux[In, (OA, OB), S0] = {
      val (a, b) = t.separate(s)
      new Both[In, OA, OB, SA, SB, S0](
        lhs.withInitialState(a),
        rhs.withInitialState(b),
        t
      )
    }
    def render: String                                                                            = s"(${lhs.render} &&& ${rhs.render})"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[(OA, OB)] { type State = S0 } = {
      // The shared source can only be read once per element. We use two
      // single-slot mailboxes (one per child) so that each pull from `source`
      // is replayed to both child Readers.
      val mb                                    = new BothMailbox[In](source)
      val l: ScanReader[OA] { type State = SA } = lhs.applyToReader(mb.leftView)
      val r: ScanReader[OB] { type State = SB } = rhs.applyToReader(mb.rightView)
      new ScanReader[(OA, OB)] {
        type State = S0
        def state: S0                              = t.combine(l.state, r.state)
        def isClosed: Boolean                      = l.isClosed && r.isClosed
        def read[A1 >: (OA, OB)](sentinel: A1): A1 = {
          val a = l.read[Any](EndOfStream)
          if (a.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
          val b = r.read[Any](EndOfStream)
          if (b.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
          (a.asInstanceOf[OA], b.asInstanceOf[OB]).asInstanceOf[A1]
        }
        def close(): Unit = {
          var firstError: Throwable = null
          try l.close()
          catch { case t: Throwable => firstError = t }
          try r.close()
          catch {
            case t: Throwable =>
              if (firstError == null) firstError = t
              else firstError.addSuppressed(t)
          }
          source.close()
          if (firstError != null) throw firstError
        }
      }
    }
  }

  /**
   * Tee shared by the two child Readers of [[Both]]. Each call to a view's
   * `read` first dequeues from its private queue; if empty, pulls one element
   * from `source` and enqueues it on the OTHER view (so when that view reads
   * next it gets the same element).
   *
   * In the common case (both sides read at the same rate), neither queue ever
   * exceeds one element. In the pathological case (one side drains the upstream
   * eagerly), the queue grows unbounded — that's a deliberate trade-off: it
   * lets `&&&` work with eager-reading children at the cost of memory. Document
   * this as a v1 limitation; element-by-element children stay O(1).
   */
  private[scan] final class BothMailbox[In](source: Reader[In]) {
    private val leftQ               = new scala.collection.mutable.ArrayDeque[Any](4)
    private val rightQ              = new scala.collection.mutable.ArrayDeque[Any](4)
    private var endReached: Boolean = false

    val leftView: Reader[In] = new Reader[In] {
      def isClosed: Boolean                = endReached && leftQ.isEmpty
      def close(): Unit                    = source.close()
      def read[A1 >: In](sentinel: A1): A1 =
        if (leftQ.nonEmpty) leftQ.removeHead().asInstanceOf[A1]
        else if (endReached) sentinel
        else {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) { endReached = true; sentinel }
          else { rightQ.append(v); v.asInstanceOf[A1] }
        }
    }

    val rightView: Reader[In] = new Reader[In] {
      def isClosed: Boolean                = endReached && rightQ.isEmpty
      def close(): Unit                    = source.close()
      def read[A1 >: In](sentinel: A1): A1 =
        if (rightQ.nonEmpty) rightQ.removeHead().asInstanceOf[A1]
        else if (endReached) sentinel
        else {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) { endReached = true; sentinel }
          else { leftQ.append(v); v.asInstanceOf[A1] }
        }
    }
  }

  // ---------------------------------------------------------------------------
  //  Choice / Choose / Parallel
  // ---------------------------------------------------------------------------

  /**
   * Choice (`+++`): Either-input, joint output. Both halves emit the same
   * output type; outputs from `left` and `right` are interleaved in the
   * order their inputs arrive.
   */
  private[scan] final class Choice[InL, InR, Out, SA, SB, S0](
    val lhs: Scan.Aux[InL, Out, SA],
    val rhs: Scan.Aux[InR, Out, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[Either[InL, InR], Out] {
    type State = S0
    def initialState: S0 = t.combine(lhs.initialState, rhs.initialState)
    def withInitialState(s: S0): Scan.Aux[Either[InL, InR], Out, S0] = {
      val (a, b) = t.separate(s)
      new Choice[InL, InR, Out, SA, SB, S0](
        lhs.withInitialState(a),
        rhs.withInitialState(b),
        t
      )
    }
    def render: String = s"(${lhs.render} +++ ${rhs.render})"
    private[scan] def applyToReader(
      source: Reader[Either[InL, InR]]
    ): ScanReader[Out] { type State = S0 } = {
      val leftDemux  = new EitherDemux[InL](source)
      val rightDemux = new EitherDemux[InR](source)
      val l          = lhs.applyToReader(leftDemux.view)
      val r          = rhs.applyToReader(rightDemux.view)
      new ScanReader[Out] {
        type State                            = S0
        def state: S0                         = t.combine(l.state, r.state)
        def isClosed: Boolean                 = source.isClosed
        def read[A1 >: Out](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else
            v.asInstanceOf[Either[InL, InR]] match {
              case Left(li)  =>
                leftDemux.enqueue(li)
                val o = l.read[Any](EndOfStream)
                if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel else o.asInstanceOf[A1]
              case Right(ri) =>
                rightDemux.enqueue(ri)
                val o = r.read[Any](EndOfStream)
                if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel else o.asInstanceOf[A1]
            }
        }
        def close(): Unit = {
          var primary: Throwable = null
          try l.close()
          catch { case t: Throwable => primary = t }
          try r.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          try source.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  /**
   * Choose (`|||`): Either-input, Either-output. `Left` inputs go to `left`
   * and produce `Left(O1)`; `Right` inputs go to `right` and produce
   * `Right(O2)`.
   */
  private[scan] final class Choose[InL, InR, OL, OR, SA, SB, S0](
    val lhs: Scan.Aux[InL, OL, SA],
    val rhs: Scan.Aux[InR, OR, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[Either[InL, InR], Either[OL, OR]] {
    type State = S0
    def initialState: S0 = t.combine(lhs.initialState, rhs.initialState)
    def withInitialState(s: S0): Scan.Aux[Either[InL, InR], Either[OL, OR], S0] = {
      val (a, b) = t.separate(s)
      new Choose[InL, InR, OL, OR, SA, SB, S0](
        lhs.withInitialState(a),
        rhs.withInitialState(b),
        t
      )
    }
    def render: String = s"(${lhs.render} ||| ${rhs.render})"
    private[scan] def applyToReader(
      source: Reader[Either[InL, InR]]
    ): ScanReader[Either[OL, OR]] { type State = S0 } = {
      val leftDemux  = new EitherDemux[InL](source)
      val rightDemux = new EitherDemux[InR](source)
      val l          = lhs.applyToReader(leftDemux.view)
      val r          = rhs.applyToReader(rightDemux.view)
      new ScanReader[Either[OL, OR]] {
        type State                                       = S0
        def state: S0                                    = t.combine(l.state, r.state)
        def isClosed: Boolean                            = source.isClosed
        def read[A1 >: Either[OL, OR]](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else
            v.asInstanceOf[Either[InL, InR]] match {
              case Left(li)  =>
                leftDemux.enqueue(li)
                val o = l.read[Any](EndOfStream)
                if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel
                else Left(o.asInstanceOf[OL]).asInstanceOf[A1]
              case Right(ri) =>
                rightDemux.enqueue(ri)
                val o = r.read[Any](EndOfStream)
                if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel
                else Right(o.asInstanceOf[OR]).asInstanceOf[A1]
            }
        }
        def close(): Unit = {
          var primary: Throwable = null
          try l.close()
          catch { case t: Throwable => primary = t }
          try r.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          try source.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  /**
   * One-shot demux buffer for [[Choice]] / [[Choose]]. The parent
   * `Choice`/`Choose` reader pulls from the upstream `Either`-stream and
   * pushes each element into the matching demux; the inner Reader simply
   * dequeues.
   */
  private[scan] final class EitherDemux[I](source: Reader[?]) {
    private val queue                = new scala.collection.mutable.ArrayDeque[I](4)
    def enqueue(v: I): Unit          = queue.append(v)
    val view: Reader[I] = new Reader[I] {
      def isClosed: Boolean                = source.isClosed && queue.isEmpty
      def close(): Unit                    = source.close()
      def read[A1 >: I](sentinel: A1): A1 =
        if (queue.nonEmpty) queue.removeHead().asInstanceOf[A1]
        else sentinel
    }
  }

  /**
   * Pair-parallel (`***`): `(InL, InR)` => `(OutL, OutR)`. Each half of the
   * input pair flows through its respective scan.
   */
  private[scan] final class Parallel[InL, InR, OL, OR, SA, SB, S0](
    val lhs: Scan.Aux[InL, OL, SA],
    val rhs: Scan.Aux[InR, OR, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[(InL, InR), (OL, OR)] {
    type State = S0
    def initialState: S0 = t.combine(lhs.initialState, rhs.initialState)
    def withInitialState(s: S0): Scan.Aux[(InL, InR), (OL, OR), S0] = {
      val (a, b) = t.separate(s)
      new Parallel[InL, InR, OL, OR, SA, SB, S0](
        lhs.withInitialState(a),
        rhs.withInitialState(b),
        t
      )
    }
    def render: String = s"(${lhs.render} *** ${rhs.render})"
    private[scan] def applyToReader(
      source: Reader[(InL, InR)]
    ): ScanReader[(OL, OR)] { type State = S0 } = {
      val leftQ  = new scala.collection.mutable.ArrayDeque[InL](4)
      val rightQ = new scala.collection.mutable.ArrayDeque[InR](4)
      val leftView: Reader[InL] = new Reader[InL] {
        def isClosed: Boolean                  = source.isClosed && leftQ.isEmpty
        def close(): Unit                      = source.close()
        def read[A1 >: InL](sentinel: A1): A1 =
          if (leftQ.nonEmpty) leftQ.removeHead().asInstanceOf[A1] else sentinel
      }
      val rightView: Reader[InR] = new Reader[InR] {
        def isClosed: Boolean                  = source.isClosed && rightQ.isEmpty
        def close(): Unit                      = source.close()
        def read[A1 >: InR](sentinel: A1): A1 =
          if (rightQ.nonEmpty) rightQ.removeHead().asInstanceOf[A1] else sentinel
      }
      val l = lhs.applyToReader(leftView)
      val r = rhs.applyToReader(rightView)
      new ScanReader[(OL, OR)] {
        type State                                = S0
        def state: S0                             = t.combine(l.state, r.state)
        def isClosed: Boolean                     = source.isClosed
        def read[A1 >: (OL, OR)](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
          val pair = v.asInstanceOf[(InL, InR)]
          leftQ.append(pair._1)
          rightQ.append(pair._2)
          val a = l.read[Any](EndOfStream)
          if (a.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
          val b = r.read[Any](EndOfStream)
          if (b.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
          (a.asInstanceOf[OL], b.asInstanceOf[OR]).asInstanceOf[A1]
        }
        def close(): Unit = {
          var primary: Throwable = null
          try l.close()
          catch { case t: Throwable => primary = t }
          try r.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          try source.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  //  Strong arrow leaves
  // ---------------------------------------------------------------------------

  /** `first[A]`: `(In, A)` => `(Out, A)`. */
  private[scan] final class First[In, A, Out, S0](inner: Scan.Aux[In, Out, S0])
      extends Scan[(In, A), (Out, A)] {
    type State = S0
    def initialState: S0                                         = inner.initialState
    def withInitialState(s: S0): Scan.Aux[(In, A), (Out, A), S0] = new First[In, A, Out, S0](inner.withInitialState(s))
    def render: String                                           = s"${inner.render}.first"
    private[scan] def applyToReader(
      source: Reader[(In, A)]
    ): ScanReader[(Out, A)] { type State = S0 } = {
      val passthrough = new scala.collection.mutable.ArrayDeque[A](4)
      val innerSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean                = source.isClosed
        def close(): Unit                    = source.close()
        def read[A1 >: In](sentinel: A1): A1 =
          if (passthrough.nonEmpty) sentinel
          else {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
            else {
              val (i, a) = v.asInstanceOf[(In, A)]
              passthrough.append(a)
              i.asInstanceOf[A1]
            }
          }
      }
      val rd = inner.applyToReader(innerSource)
      new ScanReader[(Out, A)] {
        type State                                = S0
        def state: S0                             = rd.state
        def isClosed: Boolean                     = rd.isClosed && passthrough.isEmpty
        def read[A1 >: (Out, A)](sentinel: A1): A1 = {
          val o = rd.read[Any](EndOfStream)
          if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            val a = passthrough.removeHead()
            (o.asInstanceOf[Out], a).asInstanceOf[A1]
          }
        }
        def close(): Unit = rd.close()
      }
    }
  }

  /** `second[A]`: `(A, In)` => `(A, Out)`. */
  private[scan] final class Second[In, A, Out, S0](inner: Scan.Aux[In, Out, S0])
      extends Scan[(A, In), (A, Out)] {
    type State = S0
    def initialState: S0                                         = inner.initialState
    def withInitialState(s: S0): Scan.Aux[(A, In), (A, Out), S0] = new Second[In, A, Out, S0](inner.withInitialState(s))
    def render: String                                           = s"${inner.render}.second"
    private[scan] def applyToReader(
      source: Reader[(A, In)]
    ): ScanReader[(A, Out)] { type State = S0 } = {
      val passthrough = new scala.collection.mutable.ArrayDeque[A](4)
      val innerSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean                = source.isClosed
        def close(): Unit                    = source.close()
        def read[A1 >: In](sentinel: A1): A1 =
          if (passthrough.nonEmpty) sentinel
          else {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
            else {
              val (a, i) = v.asInstanceOf[(A, In)]
              passthrough.append(a)
              i.asInstanceOf[A1]
            }
          }
      }
      val rd = inner.applyToReader(innerSource)
      new ScanReader[(A, Out)] {
        type State                                 = S0
        def state: S0                              = rd.state
        def isClosed: Boolean                      = rd.isClosed && passthrough.isEmpty
        def read[A1 >: (A, Out)](sentinel: A1): A1 = {
          val o = rd.read[Any](EndOfStream)
          if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            val a = passthrough.removeHead()
            (a, o.asInstanceOf[Out]).asInstanceOf[A1]
          }
        }
        def close(): Unit = rd.close()
      }
    }
  }

  /** Generic lens through a scan: get input, run, set output. */
  private[scan] final class Lensed[In, In2, Out, Out2, S0](
    inner: Scan.Aux[In, Out, S0],
    get: In2 => In,
    set: (In2, Out) => Out2
  ) extends Scan[In2, Out2] {
    type State = S0
    def initialState: S0                                = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In2, Out2, S0] =
      new Lensed[In, In2, Out, Out2, S0](inner.withInitialState(s), get, set)
    def render: String                                  = s"${inner.render}.lens(...)"
    private[scan] def applyToReader(
      source: Reader[In2]
    ): ScanReader[Out2] { type State = S0 } = {
      val passthrough = new scala.collection.mutable.ArrayDeque[In2](4)
      val innerSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean                = source.isClosed
        def close(): Unit                    = source.close()
        def read[A1 >: In](sentinel: A1): A1 =
          if (passthrough.nonEmpty) sentinel
          else {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
            else {
              val i2 = v.asInstanceOf[In2]
              passthrough.append(i2)
              get(i2).asInstanceOf[A1]
            }
          }
      }
      val rd = inner.applyToReader(innerSource)
      new ScanReader[Out2] {
        type State                            = S0
        def state: S0                         = rd.state
        def isClosed: Boolean                 = rd.isClosed && passthrough.isEmpty
        def read[A1 >: Out2](sentinel: A1): A1 = {
          val o = rd.read[Any](EndOfStream)
          if (o.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else {
            val i2 = passthrough.removeHead()
            set(i2, o.asInstanceOf[Out]).asInstanceOf[A1]
          }
        }
        def close(): Unit = rd.close()
      }
    }
  }

  /**
   * Semilens: `extract` may short-circuit some inputs by returning `Left(o2)`,
   * in which case those values bypass the inner scan entirely.
   */
  private[scan] final class Semilensed[In, In2, Out, Out2, S0](
    inner: Scan.Aux[In, Out, S0],
    extract: In2 => Either[Out2, In],
    inject: (In2, Out) => Out2
  ) extends Scan[In2, Out2] {
    type State = S0
    def initialState: S0                                = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In2, Out2, S0] =
      new Semilensed[In, In2, Out, Out2, S0](inner.withInitialState(s), extract, inject)
    def render: String                                  = s"${inner.render}.semilens(...)"
    private[scan] def applyToReader(
      source: Reader[In2]
    ): ScanReader[Out2] { type State = S0 } = {
      val innerQueue = new scala.collection.mutable.ArrayDeque[In](4)
      val tagQueue   = new scala.collection.mutable.ArrayDeque[In2](4)
      val innerSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean                = source.isClosed && innerQueue.isEmpty
        def close(): Unit                    = source.close()
        def read[A1 >: In](sentinel: A1): A1 =
          if (innerQueue.nonEmpty) innerQueue.removeHead().asInstanceOf[A1] else sentinel
      }
      val rd = inner.applyToReader(innerSource)
      new ScanReader[Out2] {
        type State                            = S0
        def state: S0                         = rd.state
        def isClosed: Boolean                 = source.isClosed
        def read[A1 >: Out2](sentinel: A1): A1 = {
          while (true) {
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            val i2 = v.asInstanceOf[In2]
            extract(i2) match {
              case Left(o2) => return o2.asInstanceOf[A1]
              case Right(i) =>
                innerQueue.append(i)
                tagQueue.append(i2)
                val o = rd.read[Any](EndOfStream)
                if (o.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
                val tag = tagQueue.removeHead()
                return inject(tag, o.asInstanceOf[Out]).asInstanceOf[A1]
            }
          }
          sentinel
        }
        def close(): Unit = {
          var primary: Throwable = null
          try rd.close()
          catch { case t: Throwable => primary = t }
          try source.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t
              else primary.addSuppressed(t)
          }
          if (primary != null) throw primary
        }
      }
    }
  }

  /** Output `map` wrapper. State is unchanged. */
  private[scan] final class MappedOut[In, O, O2, S0](inner: Scan.Aux[In, O, S0], f: O => O2) extends Scan[In, O2] {
    type State = S0
    def initialState: S0                                                                    = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In, O2, S0]                                       = new MappedOut[In, O, O2, S0](inner.withInitialState(s), f)
    def render: String                                                                      = s"${inner.render}.map(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[O2] { type State = S0 } = {
      val rd = inner.applyToReader(source)
      new ScanReader[O2] {
        type State = S0
        def state: S0                        = rd.state
        def isClosed: Boolean                = rd.isClosed
        def read[A1 >: O2](sentinel: A1): A1 = {
          val v = rd.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else f(v.asInstanceOf[O]).asInstanceOf[A1]
        }
        def close(): Unit = rd.close()
      }
    }
  }

  /** Input `contramap` wrapper. State is unchanged. */
  private[scan] final class MappedIn[In, In2, O, S0](inner: Scan.Aux[In, O, S0], f: In2 => In) extends Scan[In2, O] {
    type State = S0
    def initialState: S0                                                                    = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In2, O, S0]                                       = new MappedIn[In, In2, O, S0](inner.withInitialState(s), f)
    def render: String                                                                      = s"${inner.render}.contramap(...)"
    private[scan] def applyToReader(source: Reader[In2]): ScanReader[O] { type State = S0 } = {
      val mappedSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean                = source.isClosed
        def close(): Unit                    = source.close()
        def read[A1 >: In](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else f(v.asInstanceOf[In2]).asInstanceOf[A1]
        }
      }
      inner.applyToReader(mappedSource)
    }
  }

  /** State projection wrapper. */
  private[scan] final class MappedState[In, O, S, S2](inner: Scan.Aux[In, O, S], f: S => S2) extends Scan[In, O] {
    type State = S2
    def initialState: S2                             = f(inner.initialState)
    def withInitialState(s: S2): Scan.Aux[In, O, S2] =
      // mapState is a one-way projection: we cannot in general invert `f`. So
      // resuming from a projected state requires the user to hold onto the
      // pre-projection scan. We document this by keeping the inner scan at its
      // current initialState and only updating the visible state value.
      new MappedState[In, O, S, S2](inner, _ => s)
    def render: String                                                                     = s"${inner.render}.mapState(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[O] { type State = S2 } = {
      val rd = inner.applyToReader(source)
      new ScanReader[O] {
        type State = S2
        def state: S2                       = f(rd.state)
        override def jvmType: JvmType       = rd.jvmType
        def isClosed: Boolean               = rd.isClosed
        def read[A1 >: O](sentinel: A1): A1 = rd.read[A1](sentinel)
        def close(): Unit                   = rd.close()
      }
    }
  }

  // ===========================================================================
  //  Materialisation bridges
  // ===========================================================================

  /**
   * Bridge to [[Pipeline]]. Drops the state.
   *
   * The [[JvmType.Infer]] parameters are kept on the constructor so the call
   * site stays primitive-lane-aware even though Phase 1 doesn't yet dispatch on
   * them — Phase 1B will use them to pick per-lane Reader implementations for
   * the hot ops.
   */
  private[scan] final class AsPipeline[In, Out, S](
    scan: Scan.Aux[In, Out, S],
    @annotation.unused jtIn: JvmType.Infer[In @uncheckedVariance],
    @annotation.unused jtOut: JvmType.Infer[Out @uncheckedVariance]
  ) extends Pipeline[In, Out] {
    def applyToStream[E](stream: Stream[E, In]): Stream[E, Out] =
      Stream.fromReader[E, Out](scan.applyToReader(Stream.compileToReader(stream)))
    def applyToSink[E, Z](sink: Sink[E, Out, Z]): Sink[E, In, Z] =
      Pipeline.runViaSink[In, Out, E, Z](this, sink)
  }

  /** Bridge to [[Sink]]. Surfaces the final state. */
  private[scan] final class AsSink[E, In, Out, S](scan: Scan.Aux[In, Out, S]) extends Sink[E, In, S] {
    private[streams] def drain(reader: Reader[?]): S = {
      val src = reader.asInstanceOf[Reader[In]]
      val out = scan.applyToReader(src)
      try {
        var v = out.read[Any](EndOfStream)
        while (v.asInstanceOf[AnyRef] ne EndOfStream) v = out.read[Any](EndOfStream)
        out.state
      } finally out.close()
    }
  }
}
