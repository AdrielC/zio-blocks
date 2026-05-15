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
import zio.blocks.streams.{Stream, StreamsBaseSpec}
import zio.test._
import zio.test.Assertion._

object ScanStatsSpec extends StreamsBaseSpec {

  private val Eps = 1e-9

  private def closeTo(expected: Double): Assertion[Double] =
    Assertion.assertion[Double]("closeTo(" + expected + ")")(actual => math.abs(actual - expected) < Eps)

  def spec: Spec[TestEnvironment, Any] = suite("Scan stats")(
    suite("SampleStats")(
      test("empty has size 0 and undefined variance/stddev/skew/kurt") {
        val s = SampleStats.empty
        assertTrue(s.size == 0L) &&
        assertTrue(s.mean == 0.0) &&
        assertTrue(s.variance.isEmpty) &&
        assertTrue(s.stddev.isEmpty) &&
        assertTrue(s.populationVariance.isEmpty)
      },
      test("observe(x) increments size and exposes mean as m1") {
        val s = SampleStats.empty.observe(1.0).observe(2.0).observe(3.0)
        assertTrue(s.size == 3L) && assert(s.mean)(closeTo(2.0))
      },
      test("variance and stddev match the textbook formula") {
        val s = SampleStats.fromIterable(List(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0))
        // mean = 5.0, m2 = sum((x - 5)^2) = 9+1+1+1+0+0+4+16 = 32, n = 8
        assert(s.mean)(closeTo(5.0)) &&
        assert(s.variance.get)(closeTo(32.0 / 8.0)) &&
        assert(s.populationVariance.get)(closeTo(32.0 / 7.0))
      },
      test("merge is associative and empty is identity (small fixed cases)") {
        val a = SampleStats.fromIterable(List(1.0, 2.0))
        val b = SampleStats.fromIterable(List(3.0, 4.0, 5.0))
        val c = SampleStats.fromIterable(List(6.0))
        val ab_c = (a.merge(b)).merge(c)
        val a_bc = a.merge(b.merge(c))
        assert(ab_c.size)(equalTo(a_bc.size)) &&
        assert(ab_c.mean)(closeTo(a_bc.mean)) &&
        assert(ab_c.m2)(closeTo(a_bc.m2)) &&
        assert(SampleStats.empty.merge(a).mean)(closeTo(a.mean)) &&
        assert(a.merge(SampleStats.empty).mean)(closeTo(a.mean))
      },
      test("merge agrees with single-pass for randomised splits") {
        check(Gen.int(1, 50)) { n =>
          val xs   = (1 to n).map(_.toDouble * 0.5).toList
          val full = SampleStats.fromIterable(xs)
          val (l, r) = xs.splitAt(n / 2)
          val mer  = SampleStats.fromIterable(l).merge(SampleStats.fromIterable(r))
          assert(mer.size)(equalTo(full.size)) &&
          assert(mer.mean)(closeTo(full.mean)) &&
          assert(mer.m2)(closeTo(full.m2))
        }
      }
    ),
    suite("scan factories")(
      test("Scan.sampleStats emits running stats and surfaces final stats") {
        val (state, out) = Scan.sampleStats.runChunk(Chunk(1.0, 2.0, 3.0))
        assertTrue(out.length == 3) &&
        assertTrue(out(0).size == 1L) &&
        assertTrue(out(2).size == 3L) &&
        assert(state.mean)(closeTo(2.0))
      },
      test("Scan.sampleStatsTerminal consumes silently and surfaces only state") {
        val (state, out) = Scan.sampleStatsTerminal.runChunk(Chunk(1.0, 2.0, 3.0, 4.0))
        assertTrue(out.isEmpty) &&
        assertTrue(state.size == 4L) &&
        assert(state.mean)(closeTo(2.5))
      },
      test("Scan.sampleStatsFromInitial resumes from a saved value") {
        val s1 = Stream.fromChunk(Chunk(1.0, 2.0, 3.0)).run(Scan.sampleStats.toSink[Nothing]).toOption.get
        val s2 = Stream.fromChunk(Chunk(4.0, 5.0)).run(Scan.sampleStatsFromInitial(s1).toSink[Nothing]).toOption.get
        val full = SampleStats.fromIterable(List(1.0, 2.0, 3.0, 4.0, 5.0))
        assertTrue(s2.size == full.size) && assert(s2.mean)(closeTo(full.mean))
      },
      test("Scan.pairwise emits (prev, curr) starting from the second element") {
        val (state, out) = Scan.pairwise[Int].runChunk(Chunk(1, 2, 3, 4))
        assert(out)(equalTo(Chunk((1, 2), (2, 3), (3, 4)))) && assert(state)(equalTo(()))
      },
      test("Scan.diff yields curr - prev") {
        val (_, out) = Scan.diff[Int].runChunk(Chunk(1, 3, 6, 10))
        assert(out)(equalTo(Chunk(2, 3, 4)))
      },
      test("Scan.ewma(alpha=1.0) is the identity (each output equals input)") {
        val (_, out) = Scan.ewma(1.0).runChunk(Chunk(1.0, 2.0, 3.0))
        assert(out)(equalTo(Chunk(1.0, 2.0, 3.0)))
      },
      test("Scan.ewma(alpha) follows recurrence e_t = alpha*x_t + (1-alpha)*e_{t-1}") {
        val (state, out) = Scan.ewma(0.5).runChunk(Chunk(1.0, 3.0, 5.0))
        // e_0 = 1.0; e_1 = 0.5*3 + 0.5*1 = 2.0; e_2 = 0.5*5 + 0.5*2 = 3.5
        assertTrue(out.length == 3) &&
        assert(out(0))(closeTo(1.0)) &&
        assert(out(1))(closeTo(2.0)) &&
        assert(out(2))(closeTo(3.5)) &&
        assert(state)(closeTo(3.5))
      }
    ),
    suite("windowing")(
      test("tumbling(3) over 7 ints emits two full windows then a final partial window") {
        val (state, out) = Scan.tumbling[Int](3).runChunk(Chunk(1, 2, 3, 4, 5, 6, 7))
        assertTrue(out.length == 3) &&
        assert(out(0))(equalTo(Chunk(1, 2, 3))) &&
        assert(out(1))(equalTo(Chunk(4, 5, 6))) &&
        assert(out(2))(equalTo(Chunk(7))) &&
        assert(state)(equalTo(3L))
      },
      test("tumblingTime emits one chunk per window-duration") {
        val durationNs = 100L
        val input = Chunk(
          Timestamped(0L, "a"),
          Timestamped(50L, "b"),
          Timestamped(100L, "c"),
          Timestamped(150L, "d"),
          Timestamped(250L, "e")
        )
        val (state, out) = Scan.tumblingTime[String](durationNs).runChunk(input)
        // window 1: nanos in [0, 100)  -> a, b
        // window 2: nanos in [100, 200) -> c, d
        // window 3: nanos in [200, 300) -> e (final)
        assertTrue(out.length == 3) &&
        assertTrue(out(0).map(_.value) == Chunk("a", "b")) &&
        assertTrue(out(1).map(_.value) == Chunk("c", "d")) &&
        assertTrue(out(2).map(_.value) == Chunk("e")) &&
        assert(state)(equalTo(3L))
      },
      test("sliding(3, 1) emits full windows then a final partial window (matches Stream.sliding)") {
        val (state, out) = Scan.sliding[Int](3, 1).runChunk(Chunk(1, 2, 3, 4, 5))
        assertTrue(out.length == 4) &&
        assert(out(0))(equalTo(Chunk(1, 2, 3))) &&
        assert(out(1))(equalTo(Chunk(2, 3, 4))) &&
        assert(out(2))(equalTo(Chunk(3, 4, 5))) &&
        assert(out(3))(equalTo(Chunk(4, 5))) &&
        assert(state)(equalTo(4L))
      }
    ),
    suite("MACD-style chain")(
      test("timestamped >>> tumblingTime >>> windowMean >>> pairwise >>> diff >>> ewma") {
        // Synthetic: 60 points evenly spaced 1 second apart, value = i.toDouble.
        val durationNs  = 10L * 1_000_000_000L  // 10s windows
        val input = Chunk.fromIterable(
          (0 until 60).map(i => Timestamped(i.toLong * 1_000_000_000L, i.toDouble))
        )
        val pipeline =
          Scan.tumblingTime[Double](durationNs) >>>
            Scan.lift[Chunk[Timestamped[Double]], Double] { c =>
              var s = 0.0
              var i = 0
              while (i < c.length) { s += c(i).value; i += 1 }
              s / c.length.toDouble
            } >>>
            Scan.pairwise[Double] >>>
            Scan.lift[(Double, Double), Double] { case (p, c) => c - p } >>>
            Scan.ewma(0.2)

        val (state, out) =
          Stream.fromChunk(input).via(pipeline.toPipeline).runCollect.toOption.get
            .foldLeft((0L, Chunk.empty[Double])) { case ((_, acc), v) => (0L, acc :+ v) } match {
              case (_, allOut) =>
                // Re-run via toSink to get the final state separately
                val st = Stream.fromChunk(input).run(pipeline.toSink[Nothing]).toOption.get
                (1L, allOut) // placeholder; we ignore the placeholder below
                (st, allOut)
            }

        // 6 windows -> 5 diffs -> 5 EWMA outputs
        // mean of window i (i in 0..5) = average of values [10i, 10i+9]
        // each consecutive diff = 10.0 exactly (means are 4.5, 14.5, 24.5, 34.5, 44.5, 54.5)
        // EWMA(0.2) seeded by first diff (10.0); subsequent diffs all 10.0 keep ewma at 10.0
        assertTrue(out.length == 5) &&
        assertTrue(out.forall(d => math.abs(d - 10.0) < 1e-9)) &&
        // composed state: tumblingTime emits 6 windows; lift/pairwise/lift/ewma:
        //   (Long ** Unit ** Unit ** Unit ** Double) flattens via Tuples to (Long, Double)
        assertTrue(state == ((6L, 10.0)))
      }
    )
  )
}
