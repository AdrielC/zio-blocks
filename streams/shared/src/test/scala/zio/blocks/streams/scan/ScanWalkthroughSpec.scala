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

/**
 * End-to-end walkthrough of the user-facing `Scan` patterns. Doubles as
 * executable documentation for the new module — every example here matches the
 * snippets in `docs/reference/streams/scan.md`.
 */
object ScanWalkthroughSpec extends StreamsBaseSpec {

  // Domain case-class state for named accessors (the "structural"-style use
  // case that motivated the Aux pattern).
  final case class IngestSummary(bytes: Long, chunks: Long)

  def spec: Spec[TestEnvironment, Any] = suite("Scan walkthrough")(
    test("Aux ergonomics: factories produce a refined Scan.Aux") {
      // Factories return Aux so the precise state is in scope at the call
      // site for composition. Users can keep the precise type (preferred)
      // or drop it via upcast when they don't care about the state.
      val s: Scan.Aux[Int, Int, Long] = Scan.count[Int]
      val z: Either[Nothing, Long]    = Stream(1, 2, 3).run(s.toSink[Nothing])
      assert(z)(isRight(equalTo(3L)))
    },
    test("Domain case-class state via mapState") {
      // Two scans whose states are combined into a domain case class.
      // We use runningFold (Out = Long) for the byte-count side so the
      // composed Out type matches: (Long, Chunk[Byte]).
      val ingest =
        (Scan.runningFold[Chunk[Byte], Long](0L)((acc, c) => acc + c.length.toLong) &&&
          Scan.count[Chunk[Byte]]).mapState { case (b, c) => IngestSummary(b, c) }

      val payload = Chunk(Chunk.fromArray("foo".getBytes), Chunk.fromArray("bazz".getBytes))
      val state   = Stream.fromChunk(payload).run(ingest.toSink[Nothing]).toOption.get
      assert(state)(equalTo(IngestSummary(7L, 2L)))
    },
    test("Multi-summary fanout: count + running sum + running max") {
      // Fanning out three running scans -> Tuples flattens the state to
      // (Long, Int, Int). Use runningFold rather than fold (Out=Nothing
      // can't appear on the left of &&&'s output pair).
      val s = Scan.count[Int] &&&
        Scan.runningFold[Int, Int](0)(_ + _) &&&
        Scan.runningFold[Int, Int](Int.MinValue)(math.max)
      val state = Stream(2, 3, 5).run(s.toSink[Nothing]).toOption.get
      // Tuples-flattened to (Long, Int, Int): count=3, sum=10, max=5
      assert(state)(equalTo((3L, 10, 5)))
    },
    test("Resumption: pause, persist, resume across two runs") {
      val scan     = Scan.count[Int] >>> Scan.runningFold[Int, Int](0)(_ + _)
      val midState = Stream(1, 2, 3).run(scan.toSink[Nothing]).toOption.get
      val resumed  = scan.withInitialState(midState)
      val finalSt  = Stream(4, 5).run(resumed.toSink[Nothing]).toOption.get
      val oneShot  = Stream(1, 2, 3, 4, 5).run(scan.toSink[Nothing]).toOption.get
      assert(finalSt)(equalTo(oneShot))
    },
    test("Short-circuit: take(n) does not pull past n") {
      val s   = Scan.take[Int](3)
      val out = Stream.range(0, 1_000_000).via(s.toPipeline).runCollect.toOption.get
      assert(out)(equalTo(Chunk(0, 1, 2)))
    },
    test("Scope-aware: ensuring runs once on close") {
      val ran = new java.util.concurrent.atomic.AtomicInteger(0)
      val s   = Scan.ensuring(Scan.identity[Int]) { ran.incrementAndGet(); () }
      val _   = Stream(1, 2).run(s.toSink[Nothing])
      assert(ran.get())(equalTo(1))
    }
  )
}
