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

/**
 * `Scan`-flavoured combinators for time-series streams. Parity with
 * `fs2.timeseries.TimeSeries` for the parts that translate cleanly to a
 * synchronous Scan-based interpretation.
 *
 * A time series is a stream of `Timestamped[Option[A]]` where `Some` values
 * carry data and `None` values carry "tick" pulses that mark the passage of
 * time without an observation. The encoding lets downstream scans react to time
 * even when no data arrives.
 *
 * The asynchronous tick-interleaving primitives from fs2 (e.g.
 * `interpolateTicks`, `throttle`, `reorderLocally`) require an effect type and
 * are intentionally out of scope here. They belong on the `Stream` surface, not
 * on `Scan`. What we ship are the `Scan`-only adapters:
 *
 *   - [[preserve]] lifts any `Scan[I, O]` into one that operates on
 *     `Timestamped[Option[I]]` => `Timestamped[Option[O]]` — values are routed
 *     through the scan; ticks pass through unchanged.
 *   - [[preserveTicks]] is the same idea for a scan that already cares about
 *     timestamps.
 *   - [[choice]] is the fs2 `TimeSeries.choice` for Either-tagged time-series
 *     values; ticks are broadcast to both children.
 *
 * @example
 *   {{{
 *   import zio.blocks.streams.scan._
 *
 *   val running: Scan.Aux[Double, Double, Double] =
 *     Scan.runningFold[Double, Double](0.0)(_ + _)
 *
 *   val lifted: Scan.Aux[
 *     Timestamped[Option[Double]],
 *     Timestamped[Option[Double]],
 *     Double
 *   ] = TimeSeries.preserve(running)
 *   }}}
 */
object TimeSeries {

  /** A point in a time series: a value or a tick (`None`). */
  type Event[+A] = Option[A]

  /** The value at a time, or a tick if no value is present. */
  type Value[+A] = Timestamped[Event[A]]

  /**
   * Lift a [[Scan]] over plain values into one that operates on
   * `Timestamped[Option[?]]`s: values are routed through the scan and
   * re-wrapped with the original timestamp; ticks pass through unchanged.
   * Parity with `fs2.timeseries.TimeSeries.preserve`.
   */
  def preserve[In, Out, S](
    scan: Scan.Aux[In, Out, S]
  ): Scan.Aux[Value[In], Value[Out], S] =
    scan.semilens[Value[In], Value[Out]](
      tsi =>
        tsi.value match {
          case Some(v) => Right(v)
          case None    => Left(Timestamped(tsi.nanos, None))
        },
      (tsi, o) => Timestamped(tsi.nanos, Some(o))
    )

  /**
   * Lift a [[Scan]] over `Timestamped` values into one that handles ticks by
   * passing them through. Parity with
   * `fs2.timeseries.TimeSeries.preserveTicks`.
   */
  def preserveTicks[In, Out, S](
    scan: Scan.Aux[Timestamped[In], Timestamped[Out], S]
  ): Scan.Aux[Value[In], Value[Out], S] =
    scan.semilens[Value[In], Value[Out]](
      tsi =>
        tsi.value match {
          case Some(v) => Right(Timestamped(tsi.nanos, v))
          case None    => Left(Timestamped(tsi.nanos, None))
        },
      (_, tso) => Timestamped(tso.nanos, Some(tso.value))
    )

  /**
   * Combine two `Scan`s over `Value[L]` and `Value[R]` into a single scan over
   * `Value[Either[L, R]]`. Tick events (`None`) are broadcast to both children;
   * values are routed by their `Either` tag.
   *
   * State merges via [[Combine]] — same conventions as [[Scan.&&&]] and
   * [[Scan.+++]]. Outputs from the two branches must share the same `O`.
   *
   * Parity with `fs2.timeseries.TimeSeries.choice`.
   */
  def choice[L, R, O, LS, RS, S0](
    left: Scan.Aux[Value[L], O, LS],
    right: Scan.Aux[Value[R], O, RS]
  )(implicit t: Combine.Aux[LS, RS, S0]): Scan.Aux[Value[Either[L, R]], O, S0] =
    new TimeSeriesChoice[L, R, O, LS, RS, S0](left, right, t)

  // -- internal -----------------------------------------------------------

  private[scan] final class TimeSeriesChoice[L, R, O, LS, RS, S0](
    val lhs: Scan.Aux[Value[L], O, LS],
    val rhs: Scan.Aux[Value[R], O, RS],
    val t: Combine.Aux[LS, RS, S0]
  ) extends Scan[Value[Either[L, R]], O] {
    type State = S0
    def initialState: S0                                              = t.combine(lhs.initialState, rhs.initialState)
    def withInitialState(s: S0): Scan.Aux[Value[Either[L, R]], O, S0] = {
      val (a, b) = t.separate(s)
      new TimeSeriesChoice[L, R, O, LS, RS, S0](lhs.withInitialState(a), rhs.withInitialState(b), t)
    }
    def render: String = "TimeSeries.choice(" + lhs.render + ", " + rhs.render + ")"
    private[scan] def applyToReader(
      source: zio.blocks.streams.io.Reader[Value[Either[L, R]]]
    ): ScanReader[O] { type State = S0 } = {
      val leftQ                                            = new scala.collection.mutable.ArrayDeque[Value[L]](4)
      val rightQ                                           = new scala.collection.mutable.ArrayDeque[Value[R]](4)
      val outQ                                             = new scala.collection.mutable.ArrayDeque[O](4)
      val leftView: zio.blocks.streams.io.Reader[Value[L]] = new zio.blocks.streams.io.Reader[Value[L]] {
        def isClosed: Boolean                      = source.isClosed && leftQ.isEmpty
        def close(): Unit                          = source.close()
        def read[A1 >: Value[L]](sentinel: A1): A1 =
          if (leftQ.nonEmpty) leftQ.removeHead().asInstanceOf[A1] else sentinel
      }
      val rightView: zio.blocks.streams.io.Reader[Value[R]] = new zio.blocks.streams.io.Reader[Value[R]] {
        def isClosed: Boolean                      = source.isClosed && rightQ.isEmpty
        def close(): Unit                          = source.close()
        def read[A1 >: Value[R]](sentinel: A1): A1 =
          if (rightQ.nonEmpty) rightQ.removeHead().asInstanceOf[A1] else sentinel
      }
      val l = lhs.applyToReader(leftView)
      val r = rhs.applyToReader(rightView)
      new ScanReader[O] {
        type State = S0
        def state: S0                       = t.combine(l.state, r.state)
        def isClosed: Boolean               = source.isClosed && outQ.isEmpty
        def read[A1 >: O](sentinel: A1): A1 = {
          if (outQ.nonEmpty) return outQ.removeHead().asInstanceOf[A1]
          val v = source.read[Any](zio.blocks.streams.internal.EndOfStream)
          if (v.asInstanceOf[AnyRef] eq zio.blocks.streams.internal.EndOfStream) return sentinel
          val ts = v.asInstanceOf[Value[Either[L, R]]]
          ts.value match {
            case None =>
              // Tick: broadcast to both children, collect outputs.
              val tickL: Value[L] = Timestamped(ts.nanos, None)
              val tickR: Value[R] = Timestamped(ts.nanos, None)
              leftQ.append(tickL)
              rightQ.append(tickR)
              val ol = l.read[Any](zio.blocks.streams.internal.EndOfStream)
              if (ol.asInstanceOf[AnyRef] ne zio.blocks.streams.internal.EndOfStream)
                outQ.append(ol.asInstanceOf[O])
              val or = r.read[Any](zio.blocks.streams.internal.EndOfStream)
              if (or.asInstanceOf[AnyRef] ne zio.blocks.streams.internal.EndOfStream)
                outQ.append(or.asInstanceOf[O])
              if (outQ.nonEmpty) outQ.removeHead().asInstanceOf[A1]
              else read(sentinel) // tail-recursion via the outer while-true would be nicer
            case Some(Left(lv)) =>
              leftQ.append(Timestamped(ts.nanos, Some(lv)))
              val ol = l.read[Any](zio.blocks.streams.internal.EndOfStream)
              if (ol.asInstanceOf[AnyRef] eq zio.blocks.streams.internal.EndOfStream) sentinel
              else ol.asInstanceOf[A1]
            case Some(Right(rv)) =>
              rightQ.append(Timestamped(ts.nanos, Some(rv)))
              val or = r.read[Any](zio.blocks.streams.internal.EndOfStream)
              if (or.asInstanceOf[AnyRef] eq zio.blocks.streams.internal.EndOfStream) sentinel
              else or.asInstanceOf[A1]
          }
        }
        def close(): Unit = {
          var primary: Throwable = null
          try l.close()
          catch { case t: Throwable => primary = t }
          try r.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t else primary.addSuppressed(t)
          }
          try source.close()
          catch {
            case t: Throwable =>
              if (primary == null) primary = t else primary.addSuppressed(t)
          }
          if (primary != null) throw primary
        }
      }
    }
  }
}
