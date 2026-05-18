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

import zio.blocks.streams.StreamsBaseSpec
import zio.test._

/**
 * Scala 3-only compile-time tests for the inline / match-type ergonomics
 * on [[Scan]] and [[Combine]].
 *
 *   - `Combine.Merge[A, B]` reduces structurally with no priority search.
 *   - `Combine.merge[A, B]` resolves to a precise `Aux[A, B, Merge[A, B]]`
 *     for power users that want guaranteed compile-time inference.
 *   - `scan.mapState(identity)` short-circuits to `scan` (object identity),
 *     mirroring the `Sink.mapError` / `Stream.mapError` precedent.
 *
 * Most assertions live in the type system; a true regression would prevent
 * compilation rather than fail at runtime.
 */
object ScanInlineSpec extends StreamsBaseSpec {

  def spec: Spec[TestEnvironment, Any] = suite("Scan inline / match-type ergonomics (Scala 3)")(
    test("Combine.Merge[Unit, Unit] reduces to Unit") {
      summon[Combine.Merge[Unit, Unit] =:= Unit]
      assertCompletes
    },
    test("Combine.Merge[Unit, Long] reduces to Long (left identity)") {
      summon[Combine.Merge[Unit, Long] =:= Long]
      assertCompletes
    },
    test("Combine.Merge[Long, Unit] reduces to Long (right identity)") {
      summon[Combine.Merge[Long, Unit] =:= Long]
      assertCompletes
    },
    test("Combine.Merge[Long, Double] flattens via Tuples.Combined") {
      summon[Combine.Merge[Long, Double] =:= zio.blocks.combinators.Tuples.Combined[Long, Double]]
      assertCompletes
    },
    test("Combine.merge resolves runtime instances for each case") {
      val _ = summonInline[Combine.Aux[Unit, Unit, Unit]]
      val _ = summonInline[Combine.Aux[Unit, Long, Long]]
      val _ = summonInline[Combine.Aux[Long, Unit, Long]]
      assertCompletes
    },
    test("scan.mapState(identity) short-circuits to `self` (object identity)") {
      val s      = Scan.count[Int]
      val mapped = s.mapState(identity)
      // The inline summonFrom collapses `Predef.identity` to `self`.
      assertTrue(mapped eq s)
    }
  )
}
