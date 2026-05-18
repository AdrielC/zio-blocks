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

import scala.compiletime.{erasedValue, summonInline}
import zio.blocks.combinators.Tuples.Tuples

/**
 * Merges two `Scan` state types into a normalised carrier.
 *
 * On Scala 3 the result type is computed by the [[Combine.Merge]] match-type,
 * so the compiler structurally infers the composed state without doing
 * priority-based implicit search at every call site — exactly the same pattern
 * upstream uses in [[zio.blocks.combinators.Tuples.Combined]] and
 * [[zio.blocks.combinators.Eithers.CanonicalizeEither]].
 *
 * Reduction rules (read left-to-right):
 *
 *   - `Merge[Unit, Unit]` => `Unit` (avoids `Tuples.leftUnit`/`rightUnit`
 *     ambiguity)
 *   - `Merge[Unit, B]` => `B` (left identity)
 *   - `Merge[A, Unit]` => `A` (right identity)
 *   - `Merge[A, B]` => `Tuples.Combined[A, B]` (flat tuple with Scala 3
 *     auto-flattening)
 *
 * Runtime instances live in the [[Combine]] companion so implicit scope picks
 * them up without imports.
 */
trait Combine[A, B] { self =>
  type Out
  def combine(a: A, b: B): Out
  def separate(out: Out): (A, B)
}

object Combine extends CombineLowPriority {
  type Aux[A, B, O] = Combine[A, B] { type Out = O }

  /**
   * Match-type encoding of the merged state. Used as the inferred `Out` of the
   * inlined [[Combine.merge]] given so the compiler sees the precise composed
   * type at every `>>>` / `&&&` / `***` / `+++` / `|||` call site.
   *
   * Matches the upstream `Tuples.Combined` / `Eithers.CanonicalizeEither`
   * pattern (also Scala 3-only).
   */
  type Merge[A, B] = A match {
    case Unit =>
      B match {
        case Unit => Unit
        case _    => B
      }
    case _ =>
      B match {
        case Unit => A
        case _    => zio.blocks.combinators.Tuples.Combined[A, B]
      }
  }

  /**
   * Top priority: when both sides are `Unit`, the merge is `Unit` and avoids
   * the ambiguity between `Tuples.leftUnit` and `Tuples.rightUnit`.
   */
  given unitUnit: Aux[Unit, Unit, Unit] = new Combine[Unit, Unit] {
    type Out = Unit
    def combine(a: Unit, b: Unit): Unit   = ()
    def separate(out: Unit): (Unit, Unit) = ((), ())
  }

  /**
   * Compile-time-friendly inline summon that resolves to one of `unitUnit`,
   * `leftUnit`, `rightUnit`, or `fromTuples` based on `Merge[A, B]`. Avoids
   * priority-based search and surfaces the precise `Out` type via the
   * match-type — the same pattern as
   * [[zio.blocks.combinators.Eithers.Eithers.eithers]].
   *
   * This is opt-in — call sites of [[Scan.>>>]] / [[Scan.&&&]] still use the
   * regular `given`-based resolution. Power users who want guaranteed
   * compile-time inference can write
   * `summon[Combine.Aux[A, B, Combine.Merge[A, B]]]`.
   */
  inline def merge[A, B]: Combine.Aux[A, B, Merge[A, B]] =
    inline erasedValue[A] match {
      case _: Unit =>
        inline erasedValue[B] match {
          case _: Unit => unitUnit.asInstanceOf[Combine.Aux[A, B, Merge[A, B]]]
          case _       => summonInline[Combine.Aux[Unit, B, B]].asInstanceOf[Combine.Aux[A, B, Merge[A, B]]]
        }
      case _ =>
        inline erasedValue[B] match {
          case _: Unit => summonInline[Combine.Aux[A, Unit, A]].asInstanceOf[Combine.Aux[A, B, Merge[A, B]]]
          case _       =>
            // Delegate to the priority-ladder `fromTuples` given; the
            // match-type already constrains the inferred `Out` to
            // `Tuples.Combined[A, B]`, which matches Tuples' `combine`.
            summonInline[Combine[A, B]].asInstanceOf[Combine.Aux[A, B, Merge[A, B]]]
        }
    }
}

private[scan] sealed abstract class CombineLowPriority extends CombineLowestPriority {

  /** Identity: `Unit *.* B = B`. */
  given leftUnit[B]: Combine.Aux[Unit, B, B] = new Combine[Unit, B] {
    type Out = B
    def combine(a: Unit, b: B): B   = b
    def separate(out: B): (Unit, B) = ((), out)
  }

  /** Identity: `A *.* Unit = A`. */
  given rightUnit[A]: Combine.Aux[A, Unit, A] = new Combine[A, Unit] {
    type Out = A
    def combine(a: A, b: Unit): A   = a
    def separate(out: A): (A, Unit) = (out, ())
  }
}

private[scan] sealed abstract class CombineLowestPriority {

  /**
   * Lowest priority: fall through to upstream [[zio.blocks.combinators.Tuples]]
   * which provides the Scala 3 auto-flattening of nested tuples.
   */
  given fromTuples[A, B](using t: Tuples[A, B]): Combine.Aux[A, B, t.Out] = new Combine[A, B] {
    type Out = t.Out
    def combine(a: A, b: B): t.Out   = t.combine(a, b)
    def separate(out: t.Out): (A, B) = t.separate(out)
  }
}
