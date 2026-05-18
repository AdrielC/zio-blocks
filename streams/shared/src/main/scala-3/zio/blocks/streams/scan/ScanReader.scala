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

import zio.blocks.streams.io.Reader

/**
 * A [[Reader]] produced by a [[Scan]] that exposes its current/final state.
 *
 * After [[Reader.isClosed]] reports `true`, [[state]] is final and stable.
 *
 * The `State` member follows the Aux pattern used elsewhere in `zio-blocks`
 * (see `zio.blocks.schema.ToStructural` and
 * `zio.blocks.schema.binding.UnapplySeq`): the trait carries the type as a
 * member, and a refinement type alias surfaces it precisely when needed.
 */
private[scan] abstract class ScanReader[+Out] extends Reader[Out] { self =>

  /**
   * The state / summary type. Refinable via
   * `ScanReader[Out] { type State = X }`.
   */
  type State

  /** The current state. Stable after [[isClosed]] reports `true`. */
  def state: State
}
