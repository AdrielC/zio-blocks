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

import java.security.MessageDigest

import zio.blocks.chunk.Chunk
import zio.blocks.streams.JvmType
import zio.blocks.streams.internal.EndOfStream
import zio.blocks.streams.io.Reader

/**
 * JVM-only chunk-aware byte-hashing scans backed by `java.security.MessageDigest`.
 *
 * The scans operate on a `Stream[E, Chunk[Byte]]` (one chunk per pull) and
 * call `MessageDigest.update(arr, off, len)` exactly once per chunk —
 * **true bulk consumption**, not a per-byte loop. Callers with a
 * `Stream[E, Byte]` should pair with a chunking adapter that batches bytes
 * into `Chunk[Byte]`s of a chosen size before piping into a hash scan.
 *
 * @example
 *   {{{
 *   import zio.blocks.streams.{Stream, _}
 *   import zio.blocks.streams.scan._
 *   import zio.blocks.chunk.Chunk
 *
 *   val payload: Stream[Nothing, Chunk[Byte]] =
 *     Stream(Chunk.fromArray("hello ".getBytes), Chunk.fromArray("world".getBytes))
 *
 *   val digest: Either[Nothing, Chunk[Byte]] =
 *     payload.run(HashScan.digest(HashScan.HashAlgo.SHA256).toSink)
 *   }}}
 */
object HashScan {

  /**
   * The hash algorithm name passed verbatim to
   * `MessageDigest.getInstance(name)`.
   *
   * Includes the three SHA family algorithms guaranteed to be available on
   * every standard JVM by `java.security.MessageDigest`. Use
   * [[HashAlgo.Custom]] for arbitrary names supported by the host JCE
   * provider.
   */
  sealed abstract class HashAlgo(val name: String) extends Product with Serializable
  object HashAlgo {

    /** SHA-1. NIST-approved but cryptographically broken; legacy use only. */
    case object SHA1 extends HashAlgo("SHA-1")

    /** SHA-256 (FIPS 180-4). The default modern choice. */
    case object SHA256 extends HashAlgo("SHA-256")

    /** SHA-512 (FIPS 180-4). */
    case object SHA512 extends HashAlgo("SHA-512")

    /**
     * Any algorithm name supported by `java.security.MessageDigest`'s JCE
     * provider on the host JVM (e.g. `"MD5"`, `"SHA3-256"`).
     */
    final case class Custom(override val name: String) extends HashAlgo(name)
  }

  /**
   * Pass each input `Chunk[Byte]` through unchanged while updating a
   * running digest. The final digest is surfaced via the scan's state.
   *
   * Each input chunk is fed to `MessageDigest.update(arr, off, len)` in a
   * single JNI call — no per-byte loop.
   */
  def digest(algo: HashAlgo): Scan.Aux[Chunk[Byte], Chunk[Byte], Chunk[Byte]] =
    new DigestScan(algo)

  /**
   * Like [[digest]] but consumes each chunk silently (no output). Useful
   * when you only want the final digest.
   */
  def digestOnly(algo: HashAlgo): Scan.Aux[Chunk[Byte], Nothing, Chunk[Byte]] =
    new DigestOnlyScan(algo)

  // ---------------------------------------------------------------------------
  //  Implementation
  // ---------------------------------------------------------------------------

  /** Lightweight container for the live digest + cached final value. */
  private final class HashState(algo: HashAlgo) {
    val digest: MessageDigest = MessageDigest.getInstance(algo.name)
    private var finalDigest: Chunk[Byte] = Chunk.empty
    private var finalised: Boolean       = false

    def updateChunk(chunk: Chunk[Byte]): Unit = {
      val arr = chunk.toArray
      // single-call bulk update — no per-byte work
      digest.update(arr, 0, arr.length)
    }

    def finalise(): Chunk[Byte] = {
      if (!finalised) {
        finalDigest = Chunk.fromArray(digest.digest())
        finalised   = true
      }
      finalDigest
    }
  }

  private final class DigestScan(algo: HashAlgo)
      extends Scan[Chunk[Byte], Chunk[Byte]] {
    type State = Chunk[Byte]
    def initialState: Chunk[Byte] = Chunk.empty
    def withInitialState(s: Chunk[Byte]): Scan.Aux[Chunk[Byte], Chunk[Byte], Chunk[Byte]] =
      // NOTE: a true mid-stream resume of a streaming MessageDigest would
      // require persisting MessageDigest internals, which the standard
      // JCE API doesn't expose. We accept a saved final digest as a
      // *seed* — the new scan starts a fresh digest and reports `s ++
      // newDigest` only at completion if needed by the caller. For now we
      // simply return a fresh scan, ignoring the saved state, since
      // MessageDigest snapshots aren't a portable concept.
      new DigestScan(algo)
    def render: String = "HashScan.digest(" + algo.name + ")"
    private[scan] def applyToReader(source: Reader[Chunk[Byte]]): ScanReader[Chunk[Byte]] { type State = Chunk[Byte] } =
      new ScanReader[Chunk[Byte]] {
        type State                      = Chunk[Byte]
        private val st                  = new HashState(algo)
        private var sourceDone: Boolean = false
        def state: Chunk[Byte]          = if (sourceDone) st.finalise() else Chunk.empty
        def isClosed: Boolean           = source.isClosed
        override def jvmType: JvmType   = source.jvmType
        def read[A1 >: Chunk[Byte]](sentinel: A1): A1 = {
          val v = source.read[Any](EndOfStream)
          if (v.asInstanceOf[AnyRef] eq EndOfStream) {
            sourceDone = true
            sentinel
          } else {
            val c = v.asInstanceOf[Chunk[Byte]]
            st.updateChunk(c)
            c.asInstanceOf[A1]
          }
        }
        def close(): Unit = {
          if (!sourceDone) {
            sourceDone = true
            st.finalise()
          }
          source.close()
        }
      }
  }

  private final class DigestOnlyScan(algo: HashAlgo)
      extends Scan[Chunk[Byte], Nothing] {
    type State = Chunk[Byte]
    def initialState: Chunk[Byte]                                                  = Chunk.empty
    def withInitialState(s: Chunk[Byte]): Scan.Aux[Chunk[Byte], Nothing, Chunk[Byte]] =
      new DigestOnlyScan(algo)
    def render: String = "HashScan.digestOnly(" + algo.name + ")"
    private[scan] def applyToReader(source: Reader[Chunk[Byte]]): ScanReader[Nothing] { type State = Chunk[Byte] } =
      new ScanReader[Nothing] {
        type State                      = Chunk[Byte]
        private val st                  = new HashState(algo)
        private var sourceDone: Boolean = false
        def state: Chunk[Byte]          = if (sourceDone) st.finalise() else Chunk.empty
        def isClosed: Boolean           = source.isClosed
        def read[A1 >: Nothing](sentinel: A1): A1 = {
          var v = source.read[Any](EndOfStream)
          while (v.asInstanceOf[AnyRef] ne EndOfStream) {
            st.updateChunk(v.asInstanceOf[Chunk[Byte]])
            v = source.read[Any](EndOfStream)
          }
          sourceDone = true
          st.finalise()
          sentinel
        }
        def close(): Unit = {
          if (!sourceDone) {
            sourceDone = true
            st.finalise()
          }
          source.close()
        }
      }
  }
}
