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

import zio.blocks.combinators.Tuples.Tuples

/**
 * Merges two `Scan` state types into a normalised carrier.
 *
 * `Combine` is a thin priority ladder over
 * [[zio.blocks.combinators.Tuples]]. Its sole purpose is to make
 * `Scan.lift(...) &&& Scan.lift(...)` (both stateless) compile without an
 * ambiguous-implicit error: upstream `Tuples` provides both `leftUnit` and
 * `rightUnit` at the same priority, which collide when both sides are
 * `Unit`.
 *
 * Priority order:
 *   1. `unitUnit` (highest): `Combine[Unit, Unit] { type Out = Unit }`.
 *   2. `leftUnit` / `rightUnit`: identity for stateless+stateful pairs.
 *   3. `fromTuples` (lowest): delegate to upstream `Tuples`.
 *
 * Living in the `Scan` companion's package puts every instance in implicit
 * scope at the call site of `>>>` / `&&&` automatically — users never need
 * to import anything.
 */
trait Combine[A, B] { self =>
  type Out
  def combine(a: A, b: B): Out
  def separate(out: Out): (A, B)
}

object Combine extends CombineLowPriority {
  type Aux[A, B, O] = Combine[A, B] { type Out = O }

  implicit val unitUnit: Aux[Unit, Unit, Unit] = new Combine[Unit, Unit] {
    type Out = Unit
    def combine(a: Unit, b: Unit): Unit   = ()
    def separate(out: Unit): (Unit, Unit) = ((), ())
  }
}

private[scan] sealed abstract class CombineLowPriority extends CombineLowestPriority {

  implicit def leftUnit[B]: Combine.Aux[Unit, B, B] = new Combine[Unit, B] {
    type Out = B
    def combine(a: Unit, b: B): B   = b
    def separate(out: B): (Unit, B) = ((), out)
  }

  implicit def rightUnit[A]: Combine.Aux[A, Unit, A] = new Combine[A, Unit] {
    type Out = A
    def combine(a: A, b: Unit): A   = a
    def separate(out: A): (A, Unit) = (out, ())
  }
}

private[scan] sealed abstract class CombineLowestPriority {

  implicit def fromTuples[A, B](implicit t: Tuples[A, B]): Combine.Aux[A, B, t.Out] = new Combine[A, B] {
    type Out = t.Out
    def combine(a: A, b: B): t.Out   = t.combine(a, b)
    def separate(out: t.Out): (A, B) = t.separate(out)
  }
}
