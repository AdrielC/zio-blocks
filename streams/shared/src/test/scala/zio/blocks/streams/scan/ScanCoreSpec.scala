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

object ScanCoreSpec extends StreamsBaseSpec {

  // Helpers for the path-dependent State surfaced by `toSink`.
  private def runSink[In, Out, S](scan: Scan.Aux[In, Out, S], in: Chunk[In]): S =
    Stream.fromChunk(in).run(scan.toSink).fold(_ => sys.error("unexpected error"), identity)

  def spec: Spec[TestEnvironment, Any] = suite("Scan core")(
    suite("leaf factories")(
      test("identity passes elements through; state is Unit") {
        val s               = Scan.identity[Int]
        val (state, out)    = s.runChunk(Chunk(1, 2, 3))
        val pipelineOut     = Stream(1, 2, 3).via(s.toPipeline).runCollect
        val sinkState: Unit = runSink(s, Chunk(1, 2, 3))
        assert(out)(equalTo(Chunk(1, 2, 3))) &&
        assert(state)(equalTo(())) &&
        assert(sinkState)(equalTo(())) &&
        assert(pipelineOut)(isRight(equalTo(Chunk(1, 2, 3))))
      },
      test("lift maps every input through f") {
        val s            = Scan.lift[Int, Int](_ * 2)
        val (state, out) = s.runChunk(Chunk(1, 2, 3))
        assert(out)(equalTo(Chunk(2, 4, 6))) && assert(state)(equalTo(()))
      },
      test("filter drops elements not matching predicate") {
        val s            = Scan.filter[Int](_ % 2 == 0)
        val (state, out) = s.runChunk(Chunk(1, 2, 3, 4, 5, 6))
        assert(out)(equalTo(Chunk(2, 4, 6))) && assert(state)(equalTo(()))
      },
      test("collect emits only matching elements") {
        val s            = Scan.collect[Int, String] { case x if x > 0 => s"+$x" }
        val (state, out) = s.runChunk(Chunk(-1, 0, 1, 2, -3, 4))
        assert(out)(equalTo(Chunk("+1", "+2", "+4"))) && assert(state)(equalTo(()))
      },
      test("emit may produce zero or many outputs per input") {
        val s            = Scan.emit[Int, Int](n => Chunk.fromIterable(0 until n))
        val (state, out) = s.runChunk(Chunk(0, 2, 1, 3))
        assert(out)(equalTo(Chunk(0, 1, 0, 0, 1, 2))) && assert(state)(equalTo(()))
      },
      test("count surfaces the number of consumed elements as state") {
        val s            = Scan.count[String]
        val (state, out) = s.runChunk(Chunk("a", "b", "c"))
        assert(out)(equalTo(Chunk("a", "b", "c"))) && assert(state)(equalTo(3L))
      },
      test("zipWithIndex pairs each element with its 0-based index") {
        val s            = Scan.zipWithIndex[String]
        val (state, out) = s.runChunk(Chunk("a", "b", "c"))
        assert(out)(equalTo(Chunk((0L, "a"), (1L, "b"), (2L, "c")))) && assert(state)(equalTo(3L))
      },
      test("fold surfaces only the final fold value as state") {
        val s            = Scan.fold[Int, Int](0)(_ + _)
        val (state, out) = s.runChunk(Chunk(1, 2, 3, 4))
        assert(out)(isEmpty) && assert(state)(equalTo(10))
      },
      test("runningFold emits the state per element") {
        val s            = Scan.runningFold[Int, Int](0)(_ + _)
        val (state, out) = s.runChunk(Chunk(1, 2, 3, 4))
        assert(out)(equalTo(Chunk(1, 3, 6, 10))) && assert(state)(equalTo(10))
      }
    ),
    suite("composition")(
      test(">>> threads element output and merges state via Tuples (Unit identity)") {
        val s = Scan.lift[Int, Int](_ + 1) >>> Scan.count[Int]
        // Tuples leftUnit instance collapses (Unit, Long) to just Long.
        val (state, out) = s.runChunk(Chunk(10, 20, 30))
        assert(out)(equalTo(Chunk(11, 21, 31))) && assert(state)(equalTo(3L))
      },
      test(">>> with two stateful scans yields a flat tuple state") {
        val s            = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
        val (state, out) = s.runChunk(Chunk(1, 2, 3))
        assert(out)(equalTo(Chunk(1, 3, 6))) && assert(state)(equalTo((3L, 6)))
      },
      test("&&& sends every input to both scans and pairs the outputs") {
        val s            = Scan.lift[Int, Int](_ + 1) &&& Scan.lift[Int, Int](_ * 2)
        val (state, out) = s.runChunk(Chunk(1, 2, 3))
        assert(out)(equalTo(Chunk((2, 2), (3, 4), (4, 6)))) && assert(state)(equalTo(()))
      },
      test("&&& threads stateful scans into a Tuples-flattened state") {
        val s            = Scan.count[Int] &&& Scan.runningFold[Int, Int](0)(_ + _)
        val (state, out) = s.runChunk(Chunk(2, 3, 5))
        assert(out)(equalTo(Chunk((2, 2), (3, 5), (5, 10)))) && assert(state)(equalTo((3L, 10)))
      },
      test("map / contramap don't change state") {
        val s            = Scan.count[String].contramap[Int](_.toString).map(s => "[" + s + "]")
        val (state, out) = s.runChunk(Chunk(1, 2))
        assert(out)(equalTo(Chunk("[1]", "[2]"))) && assert(state)(equalTo(2L))
      },
      test("dimap composes contramap and map") {
        val s            = Scan.lift[Int, Int](_ + 1).dimap[String, String](_.toInt)(_.toString)
        val (state, out) = s.runChunk(Chunk("1", "2", "3"))
        assert(out)(equalTo(Chunk("2", "3", "4"))) && assert(state)(equalTo(()))
      }
    ),
    suite("integration with Stream/Pipeline/Sink")(
      test("Stream.via(scan.toPipeline) drops state and runs as a Pipeline") {
        val out = Stream(1, 2, 3, 4).via((Scan.lift[Int, Int](_ * 2)).toPipeline).runCollect
        assert(out)(isRight(equalTo(Chunk(2, 4, 6, 8))))
      },
      test("Stream.run(scan.toSink) returns the final state") {
        val s                       = Scan.fold[Int, Int](0)(_ + _)
        val z: Either[Nothing, Int] = Stream(1, 2, 3, 4).run(s.toSink[Nothing])
        assert(z)(isRight(equalTo(10)))
      },
      test("Stream.run on a composed scan returns a Tuples-merged state") {
        val s = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
        val z = Stream(1, 2, 3).run(s.toSink[Nothing])
        assert(z)(isRight(equalTo((3L, 6))))
      }
    ),
    suite("resumption via withInitialState")(
      test("count resumes from a saved counter") {
        val s          = Scan.count[Int]
        val mid        = runSink(s, Chunk(1, 2, 3))    // 3L
        val resumed    = s.withInitialState(mid)
        val finalCount = runSink(resumed, Chunk(4, 5)) // 5L (3 + 2)
        assert(mid)(equalTo(3L)) && assert(finalCount)(equalTo(5L))
      },
      test("runningFold resumes from a saved accumulator") {
        val s        = Scan.runningFold[Int, Int](0)(_ + _)
        val mid      = runSink(s, Chunk(1, 2, 3))    // 6
        val resumed  = s.withInitialState(mid)
        val finalAcc = runSink(resumed, Chunk(4, 5)) // 6 + 4 + 5 = 15
        assert(mid)(equalTo(6)) && assert(finalAcc)(equalTo(15))
      },
      test("composed >>> resumes by splitting the saved state via Tuples.separate") {
        val s          = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
        val mid        = runSink(s, Chunk(1, 2, 3))    // (3L, 6)
        val resumed    = s.withInitialState(mid)
        val finalState = runSink(resumed, Chunk(4, 5)) // (5L, 15)
        assert(mid)(equalTo((3L, 6))) && assert(finalState)(equalTo((5L, 15)))
      },
      test("composed &&& resumes by splitting the saved state via Tuples.separate") {
        val s          = Scan.count[Int] &&& Scan.runningFold[Int, Int](0)(_ + _)
        val mid        = runSink(s, Chunk(1, 2, 3))    // (3L, 6)
        val resumed    = s.withInitialState(mid)
        val finalState = runSink(resumed, Chunk(4, 5)) // (5L, 15)
        assert(mid)(equalTo((3L, 6))) && assert(finalState)(equalTo((5L, 15)))
      },
      test("xs ++ ys two-pass equals one-pass over xs ++ ys") {
        val s    = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
        val xs   = Chunk(1, 2, 3, 4)
        val ys   = Chunk(5, 6, 7)
        val mid  = runSink(s, xs)
        val twoP = runSink(s.withInitialState(mid), ys)
        val oneP = runSink(s, xs ++ ys)
        assert(twoP)(equalTo(oneP))
      }
    )
  )
}
