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
 * A description of a stateful transducer with typed inputs, outputs and a
 * typed state value that evolves as scans compose.
 *
 * The `State` member is hidden as a type member and refined via
 * [[Scan.Aux]] when the user wants it precise (mirroring the upstream
 * `ToStructural.Aux[A, S]` and `UnapplySeq.Aux[X, C0[_], A0]` conventions).
 * Most user code uses `Scan[In, Out]` and never names the state explicitly;
 * factories return [[Scan.Aux]] so composition has the precise state in
 * scope automatically.
 *
 * Composition (`>>>`, `&&&`) normalises the state type via
 * [[zio.blocks.combinators.Tuples]]. `Unit` is the identity, so stateless
 * scans never widen the state; two stateful scans yield a flat tuple, with
 * deeper nesting auto-flattened on Scala 3 and macro-flattened on Scala 2.
 *
 * Internal mutable state lives as `private var`s inside the `ScanReader`
 * instances each scan produces — the user never sees it.
 *
 * @tparam In
 *   Element input type (contravariant).
 * @tparam Out
 *   Element output type (covariant).
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
   */
  def withInitialState(s: State): Scan.Aux[In, Out, State]

  /** Human-readable rendering of this scan's structure. */
  def render: String

  override final def toString: String = render

  /** Build a [[ScanReader]] whose `.state` is exactly `self.State`. Internal. */
  private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = self.State }

  // ---------------------------------------------------------------------------
  //  Sequential composition
  // ---------------------------------------------------------------------------

  final def >>>[Out2, S2](
    that: Scan.Aux[Out, Out2, S2]
  )(implicit t: Combine[State, S2]): Scan.Aux[In, Out2, t.Out] =
    new Scan.AndThen[In, Out, Out2, State, S2, t.Out](self, that, t)

  final def andThen[Out2, S2](
    that: Scan.Aux[Out, Out2, S2]
  )(implicit t: Combine[State, S2]): Scan.Aux[In, Out2, t.Out] = self >>> that

  // ---------------------------------------------------------------------------
  //  Fanout: same input, paired output
  // ---------------------------------------------------------------------------

  final def &&&[Out2, S2](
    that: Scan.Aux[In @uncheckedVariance, Out2, S2]
  )(implicit t: Combine[State, S2]): Scan.Aux[In, (Out, Out2), t.Out] =
    new Scan.Both[In, Out, Out2, State, S2, t.Out](self, that, t)

  // ---------------------------------------------------------------------------
  //  Output / input transforms
  // ---------------------------------------------------------------------------

  final def map[Out2](f: Out => Out2): Scan.Aux[In, Out2, State] =
    new Scan.MappedOut[In, Out, Out2, State](self, f)

  final def contramap[In2](f: In2 => In): Scan.Aux[In2, Out, State] =
    new Scan.MappedIn[In, In2, Out, State](self, f)

  final def dimap[In2, Out2](g: In2 => In)(f: Out => Out2): Scan.Aux[In2, Out2, State] =
    contramap(g).map(f)

  // ---------------------------------------------------------------------------
  //  State projection
  // ---------------------------------------------------------------------------

  final def mapState[S2](f: State => S2): Scan.Aux[In, Out, S2] =
    new Scan.MappedState[In, Out, State, S2](self, f)

  final def asAux[S2](implicit ev: State =:= S2): Scan.Aux[In, Out, S2] = {
    val _ = ev
    self.asInstanceOf[Scan.Aux[In, Out, S2]]
  }

  // ---------------------------------------------------------------------------
  //  Materialisation
  // ---------------------------------------------------------------------------

  final def toPipeline(implicit
    jtIn: JvmType.Infer[In @uncheckedVariance],
    jtOut: JvmType.Infer[Out @uncheckedVariance]
  ): Pipeline[In, Out] = new Scan.AsPipeline[In, Out, State](self, jtIn, jtOut)

  final def toSink[E]: Sink[E, In, State] = new Scan.AsSink[E, In, Out, State](self)

  final def runChunk(in: Chunk[In]): (State, Chunk[Out]) =
    Scan.runChunkImpl[In, Out, State](self.asInstanceOf[Scan.Aux[In, Out, State]], in)
}

object Scan {

  type Aux[-In, +Out, S0] = Scan[In, Out] { type State = S0 }

  // ---------------------------------------------------------------------------
  //  Stateless leaf factories
  // ---------------------------------------------------------------------------

  def identity[A]: Scan.Aux[A, A, Unit] = new Identity[A]
  def lift[I, O](f: I => O): Scan.Aux[I, O, Unit] = new Lift[I, O](f)
  def emit[I, O](f: I => Chunk[O]): Scan.Aux[I, O, Unit] = new Emit[I, O](f)
  def filter[A](pred: A => Boolean): Scan.Aux[A, A, Unit] = new FilterScan[A](pred)
  def collect[A, B](pf: PartialFunction[A, B]): Scan.Aux[A, B, Unit] = new CollectScan[A, B](pf)

  // ---------------------------------------------------------------------------
  //  Stateful leaf factories
  // ---------------------------------------------------------------------------

  def count[A]: Scan.Aux[A, A, Long] = new Counted[A]
  def zipWithIndex[A]: Scan.Aux[A, (Long, A), Long] = new ZipWithIndex[A]

  def fold[I, S](z: S)(f: (S, I) => S): Scan.Aux[I, Nothing, S] = new FoldLeftScan[I, S](z, f)
  def runningFold[I, S](z: S)(f: (S, I) => S): Scan.Aux[I, S, S] = new RunningFoldScan[I, S](z, f)

  private[scan] def runChunkImpl[In, Out, S](scan: Scan.Aux[In, Out, S], in: Chunk[In]): (S, Chunk[Out]) = {
    val src     = Reader.fromChunk(in)(JvmType.Infer.anyRef[In]).asInstanceOf[Reader[In]]
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

  private[scan] final class Identity[A] extends Scan[A, A] {
    type State = Unit
    def initialState: Unit                              = ()
    def withInitialState(s: Unit): Scan.Aux[A, A, Unit] = this
    def render: String                                  = "Scan.identity"
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

  private[scan] final class Lift[I, O](f: I => O) extends Scan[I, O] {
    type State = Unit
    def initialState: Unit                              = ()
    def withInitialState(s: Unit): Scan.Aux[I, O, Unit] = this
    def render: String                                  = "Scan.lift(...)"
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

  private[scan] final class Emit[I, O](f: I => Chunk[O]) extends Scan[I, O] {
    type State = Unit
    def initialState: Unit                              = ()
    def withInitialState(s: Unit): Scan.Aux[I, O, Unit] = this
    def render: String                                  = "Scan.emit(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[O] { type State = Unit } =
      new ScanReader[O] {
        type State                    = Unit
        def state: Unit               = ()
        private var pending: Chunk[O] = Chunk.empty
        private var pendingIx: Int    = 0
        def isClosed: Boolean         = source.isClosed && pendingIx >= pending.length
        def read[A1 >: O](sentinel: A1): A1 = {
          while (true) {
            if (pendingIx < pending.length) {
              val o = pending(pendingIx); pendingIx += 1
              return o.asInstanceOf[A1]
            }
            val v = source.read[Any](EndOfStream)
            if (v.asInstanceOf[AnyRef] eq EndOfStream) return sentinel
            pending   = f(v.asInstanceOf[I])
            pendingIx = 0
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  private[scan] final class FilterScan[A](pred: A => Boolean) extends Scan[A, A] {
    type State = Unit
    def initialState: Unit                              = ()
    def withInitialState(s: Unit): Scan.Aux[A, A, Unit] = this
    def render: String                                  = "Scan.filter(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Unit } =
      new ScanReader[A] {
        type State                          = Unit
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

  private[scan] final class CollectScan[A, B](pf: PartialFunction[A, B]) extends Scan[A, B] {
    type State = Unit
    def initialState: Unit                              = ()
    def withInitialState(s: Unit): Scan.Aux[A, B, Unit] = this
    def render: String                                  = "Scan.collect(...)"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[B] { type State = Unit } =
      new ScanReader[B] {
        type State                          = Unit
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

  private[scan] final class Counted[A] private (initial: Long) extends Scan[A, A] {
    def this() = this(0L)
    type State = Long
    def initialState: Long                              = initial
    def withInitialState(s: Long): Scan.Aux[A, A, Long] = new Counted[A](s)
    def render: String                                  = "Scan.count"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[A] { type State = Long } =
      new ScanReader[A] {
        type State                          = Long
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

  private[scan] final class ZipWithIndex[A] private (initial: Long) extends Scan[A, (Long, A)] {
    def this() = this(0L)
    type State = Long
    def initialState: Long                                      = initial
    def withInitialState(s: Long): Scan.Aux[A, (Long, A), Long] = new ZipWithIndex[A](s)
    def render: String                                          = "Scan.zipWithIndex"
    private[scan] def applyToReader(source: Reader[A]): ScanReader[(Long, A)] { type State = Long } =
      new ScanReader[(Long, A)] {
        type State                                  = Long
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

  private[scan] final class FoldLeftScan[I, S](z: S, f: (S, I) => S) extends Scan[I, Nothing] {
    type State = S
    def initialState: S                                 = z
    def withInitialState(s: S): Scan.Aux[I, Nothing, S] = new FoldLeftScan[I, S](s, f)
    def render: String                                  = "Scan.fold(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[Nothing] { type State = S } =
      new ScanReader[Nothing] {
        type State                                = S
        private var acc: S                        = z
        def state: S                              = acc
        def isClosed: Boolean                     = source.isClosed
        def read[A1 >: Nothing](sentinel: A1): A1 = {
          var v = source.read[Any](EndOfStream)
          while (v.asInstanceOf[AnyRef] ne EndOfStream) {
            acc = f(acc, v.asInstanceOf[I])
            v   = source.read[Any](EndOfStream)
          }
          sentinel
        }
        def close(): Unit = source.close()
      }
  }

  private[scan] final class RunningFoldScan[I, S](z: S, f: (S, I) => S) extends Scan[I, S] {
    type State = S
    def initialState: S                           = z
    def withInitialState(s: S): Scan.Aux[I, S, S] = new RunningFoldScan[I, S](s, f)
    def render: String                            = "Scan.runningFold(...)"
    private[scan] def applyToReader(source: Reader[I]): ScanReader[S] { type State = S } =
      new ScanReader[S] {
        type State                          = S
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
  //  Composition wrappers
  // ===========================================================================

  private[scan] final class AndThen[In, Mid, Out, SA, SB, S0](
    val first: Scan.Aux[In, Mid, SA],
    val second: Scan.Aux[Mid, Out, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[In, Out] {
    type State = S0
    def initialState: S0 = t.combine(first.initialState, second.initialState)
    def withInitialState(s: S0): Scan.Aux[In, Out, S0] = {
      val (a, b) = t.separate(s)
      new AndThen[In, Mid, Out, SA, SB, S0](
        first.withInitialState(a),
        second.withInitialState(b),
        t
      )
    }
    def render: String = first.render + " >>> " + second.render
    private[scan] def applyToReader(source: Reader[In]): ScanReader[Out] { type State = S0 } = {
      val mid: ScanReader[Mid] { type State = SA } = first.applyToReader(source)
      val out: ScanReader[Out] { type State = SB } = second.applyToReader(mid)
      new ScanReader[Out] {
        type State                            = S0
        def state: S0                         = t.combine(mid.state, out.state)
        override def jvmType: JvmType         = out.jvmType
        def isClosed: Boolean                 = out.isClosed
        def read[A1 >: Out](sentinel: A1): A1 = out.read[A1](sentinel)
        def close(): Unit                     = out.close()
      }
    }
  }

  private[scan] final class Both[In, OA, OB, SA, SB, S0](
    val left: Scan.Aux[In, OA, SA],
    val right: Scan.Aux[In, OB, SB],
    val t: Combine.Aux[SA, SB, S0]
  ) extends Scan[In, (OA, OB)] {
    type State = S0
    def initialState: S0 = t.combine(left.initialState, right.initialState)
    def withInitialState(s: S0): Scan.Aux[In, (OA, OB), S0] = {
      val (a, b) = t.separate(s)
      new Both[In, OA, OB, SA, SB, S0](
        left.withInitialState(a),
        right.withInitialState(b),
        t
      )
    }
    def render: String = "(" + left.render + " &&& " + right.render + ")"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[(OA, OB)] { type State = S0 } = {
      val mb                                    = new BothMailbox[In](source)
      val l: ScanReader[OA] { type State = SA } = left.applyToReader(mb.leftView)
      val r: ScanReader[OB] { type State = SB } = right.applyToReader(mb.rightView)
      new ScanReader[(OA, OB)] {
        type State                                 = S0
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

  private[scan] final class BothMailbox[In](source: Reader[In]) {
    private var slot: Any            = null
    private var slotPresent: Boolean = false
    private var leftSeen: Boolean    = true
    private var rightSeen: Boolean   = true
    private var endReached: Boolean  = false

    private def fillIfNeeded(): Unit =
      if (leftSeen && rightSeen && !endReached) {
        val v = source.read[Any](EndOfStream)
        if (v.asInstanceOf[AnyRef] eq EndOfStream) {
          endReached  = true
          slotPresent = false
        } else {
          slot        = v
          slotPresent = true
          leftSeen    = false
          rightSeen   = false
        }
      }

    val leftView: Reader[In] = new Reader[In] {
      def isClosed: Boolean = endReached && slotPresent == false
      def close(): Unit     = source.close()
      def read[A1 >: In](sentinel: A1): A1 = {
        fillIfNeeded()
        if (!slotPresent) sentinel
        else {
          leftSeen = true
          val v    = slot
          if (rightSeen) { slotPresent = false; slot = null }
          v.asInstanceOf[A1]
        }
      }
    }

    val rightView: Reader[In] = new Reader[In] {
      def isClosed: Boolean = endReached && slotPresent == false
      def close(): Unit     = source.close()
      def read[A1 >: In](sentinel: A1): A1 = {
        fillIfNeeded()
        if (!slotPresent) sentinel
        else {
          rightSeen = true
          val v     = slot
          if (leftSeen) { slotPresent = false; slot = null }
          v.asInstanceOf[A1]
        }
      }
    }
  }

  private[scan] final class MappedOut[In, O, O2, S0](inner: Scan.Aux[In, O, S0], f: O => O2)
    extends Scan[In, O2] {
    type State = S0
    def initialState: S0                              = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In, O2, S0] = new MappedOut[In, O, O2, S0](inner.withInitialState(s), f)
    def render: String                                = inner.render + ".map(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[O2] { type State = S0 } = {
      val rd = inner.applyToReader(source)
      new ScanReader[O2] {
        type State                           = S0
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

  private[scan] final class MappedIn[In, In2, O, S0](inner: Scan.Aux[In, O, S0], f: In2 => In)
    extends Scan[In2, O] {
    type State = S0
    def initialState: S0                              = inner.initialState
    def withInitialState(s: S0): Scan.Aux[In2, O, S0] = new MappedIn[In, In2, O, S0](inner.withInitialState(s), f)
    def render: String                                = inner.render + ".contramap(...)"
    private[scan] def applyToReader(source: Reader[In2]): ScanReader[O] { type State = S0 } = {
      val mappedSource: Reader[In] = new Reader[In] {
        def isClosed: Boolean = source.isClosed
        def close(): Unit     = source.close()
        def read[A1 >: In](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) sentinel
          else f(v.asInstanceOf[In2]).asInstanceOf[A1]
        }
      }
      inner.applyToReader(mappedSource)
    }
  }

  private[scan] final class MappedState[In, O, S, S2](inner: Scan.Aux[In, O, S], f: S => S2)
    extends Scan[In, O] {
    type State = S2
    def initialState: S2 = f(inner.initialState)
    def withInitialState(s: S2): Scan.Aux[In, O, S2] =
      new MappedState[In, O, S, S2](inner, _ => s)
    def render: String = inner.render + ".mapState(...)"
    private[scan] def applyToReader(source: Reader[In]): ScanReader[O] { type State = S2 } = {
      val rd = inner.applyToReader(source)
      new ScanReader[O] {
        type State                          = S2
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

  private[scan] final class AsPipeline[In, Out, S](
    scan: Scan.Aux[In, Out, S],
    @scala.annotation.unused jtIn: JvmType.Infer[In @uncheckedVariance],
    @scala.annotation.unused jtOut: JvmType.Infer[Out @uncheckedVariance]
  ) extends Pipeline[In, Out] {
    def applyToStream[E](stream: Stream[E, In]): Stream[E, Out] =
      Stream.fromReader[E, Out](scan.applyToReader(Stream.compileToReader(stream)))
    def applyToSink[E, Z](sink: Sink[E, Out, Z]): Sink[E, In, Z] =
      Pipeline.runViaSink[In, Out, E, Z](this, sink)
  }

  private[scan] final class AsSink[E, In, Out, S](scan: Scan.Aux[In, Out, S]) extends Sink[E, In, S] {
    private[streams] def drain(reader: Reader[_]): S = {
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
