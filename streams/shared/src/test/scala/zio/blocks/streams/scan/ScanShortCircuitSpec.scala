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

object ScanShortCircuitSpec extends StreamsBaseSpec {

  def spec: Spec[TestEnvironment, Any] = suite("Scan short-circuit")(
    suite("take")(
      test("take(n) emits at most n elements and surfaces the count") {
        val (state, out) = Scan.take[Int](3).runChunk(Chunk(1, 2, 3, 4, 5))
        assert(out)(equalTo(Chunk(1, 2, 3))) && assert(state)(equalTo(3L))
      },
      test("take(0) emits nothing and reports 0") {
        val (state, out) = Scan.take[Int](0).runChunk(Chunk(1, 2, 3))
        assert(out)(isEmpty) && assert(state)(equalTo(0L))
      },
      test("take(n) on a Stream propagates the limit") {
        val out = Stream.range(0, 1_000_000).via(Scan.take[Int](5).toPipeline).runCollect
        assert(out)(isRight(equalTo(Chunk(0, 1, 2, 3, 4))))
      }
    ),
    suite("takeWhile / haltWhen")(
      test("takeWhile keeps the prefix where pred holds") {
        val (state, out) = Scan.takeWhile[Int](_ < 4).runChunk(Chunk(1, 2, 3, 4, 5, 1))
        assert(out)(equalTo(Chunk(1, 2, 3))) && assert(state)(equalTo(3L))
      },
      test("haltWhen excludes the matching element") {
        val (state, out) = Scan.haltWhen[Int](_ == 4).runChunk(Chunk(1, 2, 3, 4, 5))
        assert(out)(equalTo(Chunk(1, 2, 3))) && assert(state)(equalTo(3L))
      }
    ),
    suite("drop / dropWhile")(
      test("drop(n) skips the first n elements") {
        val (state, out) = Scan.drop[Int](2).runChunk(Chunk(1, 2, 3, 4, 5))
        assert(out)(equalTo(Chunk(3, 4, 5))) && assert(state)(equalTo(2L))
      },
      test("dropWhile skips the longest matching prefix") {
        val (state, out) = Scan.dropWhile[Int](_ < 3).runChunk(Chunk(1, 2, 3, 4, 1))
        assert(out)(equalTo(Chunk(3, 4, 1))) && assert(state)(equalTo(2L))
      }
    ),
    suite("ensuring")(
      test("finalizer runs exactly once on success") {
        var ran = 0
        val s   = Scan.ensuring(Scan.identity[Int])(ran += 1)
        val _   = s.runChunk(Chunk(1, 2, 3))
        assert(ran)(equalTo(1))
      },
      test("finalizer runs exactly once on short-circuit (take(0))") {
        var ran = 0
        val s   = Scan.ensuring(Scan.take[Int](0))(ran += 1)
        val _   = s.runChunk(Chunk(1, 2, 3))
        assert(ran)(equalTo(1))
      },
      test("finalizer is composable through >>>") {
        var ran        = 0
        val s          = Scan.ensuring(Scan.identity[Int])(ran += 1) >>> Scan.count[Int]
        val (state, _) = s.runChunk(Chunk(1, 2, 3))
        assert(ran)(equalTo(1)) && assert(state)(equalTo(3L))
      }
    ),
    suite("acquireRelease")(
      test("acquire runs once and release runs on close") {
        var acquireCount = 0
        var releaseCount = 0
        val scan         = Scan.acquireRelease[Int, Int, Long, String](
          { acquireCount += 1; "resource" },
          (_: String) => releaseCount += 1
        )(_ => Scan.count[Int])
        val (state, out) = scan.runChunk(Chunk(10, 20, 30))
        assert(out)(equalTo(Chunk(10, 20, 30))) &&
        assert(state._1)(equalTo("resource")) &&
        assert(state._2)(equalTo(3L)) &&
        assert(acquireCount)(equalTo(1)) &&
        assert(releaseCount)(equalTo(1))
      },
      test("release runs even when downstream short-circuits") {
        var releaseCount = 0
        val scan         = Scan.acquireRelease[Int, Int, Long, String](
          "r",
          (_: String) => releaseCount += 1
        )(_ => Scan.take[Int](2))
        val (state, out) = scan.runChunk(Chunk(1, 2, 3, 4))
        assert(out)(equalTo(Chunk(1, 2))) &&
        assert(state._2)(equalTo(2L)) &&
        assert(releaseCount)(equalTo(1))
      }
    )
  )
}
