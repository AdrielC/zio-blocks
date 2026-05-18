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

import zio.blocks.chunk.Chunk
import zio.blocks.streams.StreamsBaseSpec
import zio.test._
import zio.test.Assertion._

/**
 * Tests for the [[TimeSeries]] `Scan` combinators (parity subset with
 * `fs2.timeseries.TimeSeries`).
 */
object TimeSeriesSpec extends StreamsBaseSpec {

  // Helpers
  private def value[A](nanos: Long, a: A): TimeSeries.Value[A] = Timestamped(nanos, Some(a))
  private def tick[A](nanos: Long): TimeSeries.Value[A]        = Timestamped(nanos, None)

  def spec: Spec[TestEnvironment, Any] = suite("TimeSeries (Scan)")(
    suite("preserve")(
      test("values route through the scan; ticks pass through unchanged") {
        val running = Scan.runningFold[Int, Int](0)(_ + _)
        val lifted  = TimeSeries.preserve(running)
        val input   = Chunk(
          value(0L, 1),
          tick[Int](10L),
          value(20L, 2),
          tick[Int](30L),
          value(40L, 3)
        )
        val (state, out) = lifted.runChunk(input)
        assert(out)(
          equalTo(
            Chunk(
              value(0L, 1),
              tick[Int](10L),
              value(20L, 3),
              tick[Int](30L),
              value(40L, 6)
            )
          )
        ) && assert(state)(equalTo(6))
      },
      test("preserve preserves timestamps on routed values") {
        val lifted   = TimeSeries.preserve(Scan.lift[Int, Int](_ * 10))
        val input    = Chunk(value(100L, 5), value(200L, 7))
        val (_, out) = lifted.runChunk(input)
        assert(out)(equalTo(Chunk(value(100L, 50), value(200L, 70))))
      }
    ),
    suite("preserveTicks")(
      test("delegates to a Timestamped-aware scan") {
        // Scan that bumps the value AND advances the timestamp by 1 ns.
        val inner: Scan.Aux[Timestamped[Int], Timestamped[Int], Unit] =
          Scan.lift[Timestamped[Int], Timestamped[Int]](ts => Timestamped(ts.nanos + 1L, ts.value + 100))
        val lifted   = TimeSeries.preserveTicks(inner)
        val input    = Chunk(value(10L, 1), tick[Int](20L), value(30L, 2))
        val (_, out) = lifted.runChunk(input)
        assert(out)(
          equalTo(
            Chunk(
              value(11L, 101),
              tick[Int](20L),
              value(31L, 102)
            )
          )
        )
      }
    ),
    suite("choice")(
      test("routes Left/Right by tag; ticks broadcast to both sides") {
        // Two running sums, one over Lefts (Int) and one over Rights (Int).
        val leftScan =
          TimeSeries
            .preserve(Scan.runningFold[Int, Int](0)(_ + _))
            .map(_.value.getOrElse(0))
        val rightScan =
          TimeSeries
            .preserve(Scan.runningFold[Int, Int](0)(_ + _))
            .map(_.value.getOrElse(0))
        val combined = TimeSeries.choice[Int, Int, Int, Int, Int, (Int, Int)](
          leftScan,
          rightScan
        )
        val input = Chunk[TimeSeries.Value[Either[Int, Int]]](
          value(0L, Left(1)),
          value(10L, Right(10)),
          value(20L, Left(2)),
          value(30L, Right(20))
        )
        val (state, out) = combined.runChunk(input)
        // Left running-sum: 1, then 3 -> outputs 1, 3 (on Left arrivals)
        // Right running-sum: 10, then 30 -> outputs 10, 30 (on Right arrivals)
        assert(out)(equalTo(Chunk(1, 10, 3, 30))) &&
        assert(state)(equalTo((3, 30)))
      }
    )
  )
}
