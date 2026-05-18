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
 * Arrow-class parity tests. Mirrors fs2's `Scan` surface:
 * `first`, `second`, `***`, `lens`, `semilens`, `semipass`, `left`,
 * `right`, `+++` (choice), `|||` (choose), `step`.
 */
object ScanArrowSpec extends StreamsBaseSpec {

  def spec: Spec[TestEnvironment, Any] = suite("Scan arrow parity")(
    suite("strong (first / second)")(
      test("first lifts a scan over the left side of a pair") {
        val s = Scan.runningFold[Int, Int](0)(_ + _).first[String]
        val (state, out) = s.runChunk(Chunk((1, "a"), (2, "b"), (3, "c")))
        assert(out)(equalTo(Chunk((1, "a"), (3, "b"), (6, "c")))) &&
        assert(state)(equalTo(6))
      },
      test("second lifts a scan over the right side of a pair") {
        val s = Scan.runningFold[Int, Int](0)(_ + _).second[String]
        val (state, out) = s.runChunk(Chunk(("a", 1), ("b", 2), ("c", 3)))
        assert(out)(equalTo(Chunk(("a", 1), ("b", 3), ("c", 6)))) &&
        assert(state)(equalTo(6))
      },
      test("first commutes with arr(swap) (arrow law)") {
        // first(f) >>> arr(swap) == arr(swap) >>> second(f)
        val f = Scan.lift[Int, Int](_ + 1)
        val firstThenSwap  =
          f.first[String] >>> Scan.lift[(Int, String), (String, Int)] { case (i, s) => (s, i) }
        val swapThenSecond =
          Scan.lift[(Int, String), (String, Int)] { case (i, s) => (s, i) } >>> f.second[String]
        val (_, l) = firstThenSwap.runChunk(Chunk((1, "a"), (2, "b")))
        val (_, r) = swapThenSecond.runChunk(Chunk((1, "a"), (2, "b")))
        assert(l)(equalTo(r))
      }
    ),
    suite("*** (split)")(
      test("each side of the input pair flows through its respective scan") {
        val s = Scan.lift[Int, Int](_ + 1) *** Scan.lift[String, String](_.toUpperCase)
        val (_, out) = s.runChunk(Chunk((1, "a"), (2, "b"), (3, "c")))
        assert(out)(equalTo(Chunk((2, "A"), (3, "B"), (4, "C"))))
      },
      test("state merges via Combine when both sides are stateful") {
        val s = Scan.count[Int] *** Scan.runningFold[String, Int](0)(_ + _.length)
        val (state, out) = s.runChunk(Chunk((1, "a"), (2, "bb"), (3, "ccc")))
        assert(out)(equalTo(Chunk((1, 1), (2, 3), (3, 6)))) &&
        assert(state)(equalTo((3L, 6)))
      }
    ),
    suite("lens / semilens / semipass")(
      test("lens extracts In from I2, runs scan, recombines with set") {
        case class Wrap(label: String, value: Int)
        val s = Scan.runningFold[Int, Int](0)(_ + _)
          .lens[Wrap, Wrap](_.value, (w, sum) => w.copy(value = sum))
        val (state, out) = s.runChunk(Chunk(Wrap("a", 1), Wrap("b", 2), Wrap("c", 3)))
        assert(out)(equalTo(Chunk(Wrap("a", 1), Wrap("b", 3), Wrap("c", 6)))) &&
        assert(state)(equalTo(6))
      },
      test("semilens may short-circuit some inputs by returning Left") {
        // Only non-negative numbers flow through the running-fold;
        // negatives pass straight through as `-1` markers.
        val s = Scan.runningFold[Int, Int](0)(_ + _)
          .semilens[Int, Int]({ i =>
            if (i < 0) Left(-1) else Right(i)
          }, (_, sum) => sum)
        val (state, out) = s.runChunk(Chunk(1, -5, 2, -7, 3))
        assert(out)(equalTo(Chunk(1, -1, 3, -1, 6))) &&
        assert(state)(equalTo(6))
      },
      test("semipass passes the scan's output through unchanged") {
        val s = Scan.runningFold[Int, Int](0)(_ + _)
          .semipass[Int, Int](i => if (i < 0) Left(0) else Right(i))
        val (_, out) = s.runChunk(Chunk(1, -1, 2, -1, 3))
        assert(out)(equalTo(Chunk(1, 0, 3, 0, 6)))
      }
    ),
    suite("Either-input (left / right / +++ / |||)")(
      test("left routes Left through the scan; Right values pass through") {
        val s = Scan.runningFold[Int, Int](0)(_ + _).left[String]
        val input = Chunk[Either[Int, String]](Left(1), Right("a"), Left(2), Right("b"), Left(3))
        val (state, out) = s.runChunk(input)
        assert(out)(equalTo(Chunk(Left(1), Right("a"), Left(3), Right("b"), Left(6)))) &&
        assert(state)(equalTo(6))
      },
      test("right routes Right through the scan; Left values pass through") {
        val s = Scan.runningFold[Int, Int](0)(_ + _).right[String]
        val input = Chunk[Either[String, Int]](Right(1), Left("a"), Right(2), Left("b"), Right(3))
        val (state, out) = s.runChunk(input)
        assert(out)(equalTo(Chunk(Right(1), Left("a"), Right(3), Left("b"), Right(6)))) &&
        assert(state)(equalTo(6))
      },
      test("+++ (choice) routes each side to its scan; outputs share a type") {
        val s = Scan.lift[Int, Int](_ * 2) +++ Scan.lift[String, Int](_.length)
        val input = Chunk[Either[Int, String]](Left(5), Right("ab"), Left(10), Right("xyz"))
        val (_, out) = s.runChunk(input)
        assert(out)(equalTo(Chunk(10, 2, 20, 3)))
      },
      test("||| (choose) routes each side to its scan; outputs in an Either") {
        val s = Scan.lift[Int, Int](_ + 1) ||| Scan.lift[String, String](_.toUpperCase)
        val input = Chunk[Either[Int, String]](Left(1), Right("a"), Left(2), Right("b"))
        val (_, out) = s.runChunk(input)
        assert(out)(equalTo(Chunk[Either[Int, String]](Left(2), Right("A"), Left(3), Right("B"))))
      }
    ),
    suite("step (single-element parity with fs2)")(
      test("step advances state and returns outputs") {
        val s              = Scan.runningFold[Int, Int](0)(_ + _)
        val (next, out1)   = s.step(5)
        val (next2, out2)  = next.step(10)
        assert(out1)(equalTo(Chunk(5))) &&
        assert(out2)(equalTo(Chunk(15))) &&
        assert(next2.initialState)(equalTo(15))
      }
    )
  )
}
