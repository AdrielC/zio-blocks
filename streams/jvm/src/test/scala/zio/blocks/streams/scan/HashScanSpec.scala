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
import zio.blocks.streams.{Stream, StreamsBaseSpec}
import zio.test._
import zio.test.Assertion._

object HashScanSpec extends StreamsBaseSpec {

  private def reference(algo: String, bytes: Array[Byte]): Chunk[Byte] =
    Chunk.fromArray(MessageDigest.getInstance(algo).digest(bytes))

  private def chunkedBytes(bytes: Array[Byte], chunkSize: Int): List[Chunk[Byte]] = {
    val builder = List.newBuilder[Chunk[Byte]]
    var i       = 0
    while (i < bytes.length) {
      val end = math.min(i + chunkSize, bytes.length)
      builder += Chunk.fromArray(bytes.slice(i, end))
      i = end
    }
    builder.result()
  }

  def spec: Spec[TestEnvironment, Any] = suite("HashScan (JVM)")(
    suite("digest")(
      test("matches MessageDigest for empty input") {
        val state =
          Stream.fromChunk(Chunk.empty[Chunk[Byte]])
            .run(HashScan.digest(HashScan.HashAlgo.SHA256).toSink[Nothing])
            .toOption
            .get
        assert(state)(equalTo(reference("SHA-256", Array.emptyByteArray)))
      },
      test("matches MessageDigest for a single chunk") {
        val payload = "hello, world".getBytes("UTF-8")
        val state =
          Stream(Chunk.fromArray(payload))
            .run(HashScan.digest(HashScan.HashAlgo.SHA256).toSink[Nothing])
            .toOption
            .get
        assert(state)(equalTo(reference("SHA-256", payload)))
      },
      test("matches MessageDigest for many small chunks (1-byte chunks)") {
        val payload = "the quick brown fox jumps over the lazy dog".getBytes("UTF-8")
        val state =
          Stream.fromIterable(chunkedBytes(payload, 1))
            .run(HashScan.digest(HashScan.HashAlgo.SHA256).toSink[Nothing])
            .toOption
            .get
        assert(state)(equalTo(reference("SHA-256", payload)))
      },
      test("matches MessageDigest across multiple algorithms and chunk sizes") {
        val rng     = new scala.util.Random(0xC0FFEE)
        val payload = Array.fill[Byte](64 * 1024)(0)
        rng.nextBytes(payload)

        val algos = List(
          HashScan.HashAlgo.SHA1   -> "SHA-1",
          HashScan.HashAlgo.SHA256 -> "SHA-256",
          HashScan.HashAlgo.SHA512 -> "SHA-512"
        )
        val chunkSizes = List(1, 7, 64, 1024, 4096, payload.length)

        val results = for {
          (algo, name) <- algos
          chunkSize    <- chunkSizes
        } yield {
          val state =
            Stream.fromIterable(chunkedBytes(payload, chunkSize))
              .run(HashScan.digest(algo).toSink[Nothing])
              .toOption
              .get
          (algo, chunkSize, state == reference(name, payload))
        }

        assertTrue(results.forall(_._3))
      },
      test("output passes Chunk[Byte]s through unchanged") {
        val chunks = List(
          Chunk.fromArray("foo".getBytes),
          Chunk.fromArray("bar".getBytes),
          Chunk.fromArray("baz".getBytes)
        )
        val out =
          Stream.fromIterable(chunks)
            .via(HashScan.digest(HashScan.HashAlgo.SHA256).toPipeline)
            .runCollect
            .toOption
            .get
        assertTrue(out == Chunk.fromIterable(chunks))
      }
    ),
    suite("digestOnly")(
      test("emits no elements but surfaces the same digest as digest") {
        val payload = "hello".getBytes
        val state =
          Stream(Chunk.fromArray(payload))
            .run(HashScan.digestOnly(HashScan.HashAlgo.SHA256).toSink[Nothing])
            .toOption
            .get
        assert(state)(equalTo(reference("SHA-256", payload)))
      }
    ),
    suite("composition")(
      test("digest &&& count surfaces both the digest and the chunk count") {
        val chunks = List.tabulate(5)(i => Chunk.fromArray(s"chunk$i".getBytes))
        val scan   = HashScan.digest(HashScan.HashAlgo.SHA256) &&& Scan.count[Chunk[Byte]]
        val state  = Stream.fromIterable(chunks).run(scan.toSink[Nothing]).toOption.get
        val refDigest = reference("SHA-256", chunks.map(_.toArray).foldLeft(Array.empty[Byte])(_ ++ _))
        assert(state._1)(equalTo(refDigest)) && assert(state._2)(equalTo(5L))
      }
    )
  )
}
