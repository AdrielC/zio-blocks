package zio.blocks.blobstore

import zio.NonEmptyChunk
import zio.test._
import zio.test.Assertion._

object BlobStoreSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("BlobStoreSpec")(
      test("put/get round-trips bytes") {
        val store      = BlobStore.inMemory
        val Right(key) = BlobKey.either("a/b/c.bin"): @unchecked
        val bytes      = Array[Byte](1, 2, 3, 4)

        assert(store.put(key, bytes))(isRight(hasField("size", (_: BlobMetadata).size, equalTo(4L)))) &&
        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(bytes.toVector))))
      },
      test("put defensively copies the input array") {
        val store      = BlobStore.inMemory
        val Right(key) = BlobKey.either("x"): @unchecked
        val bytes      = Array[Byte](1, 2, 3)

        store.put(key, bytes)
        bytes(0) = 9

        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(Array[Byte](1, 2, 3).toVector))))
      },
      test("get defensively copies the stored array") {
        val store      = BlobStore.inMemory
        val Right(key) = BlobKey.either("x"): @unchecked

        store.put(key, Array[Byte](1, 2, 3))
        val Right(Some(out)) = store.get(key): @unchecked
        out(0) = 9

        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(Array[Byte](1, 2, 3).toVector))))
      },
      test("head returns metadata without content") {
        val store      = BlobStore.inMemory
        val Right(key) = BlobKey.either("k"): @unchecked

        store.put(key, Array.fill[Byte](10)(42))

        assert(store.head(key))(isRight(isSome(equalTo(BlobMetadata(key, 10L)))))
      },
      test("delete returns whether the key existed") {
        val store      = BlobStore.inMemory
        val Right(key) = BlobKey.either("k"): @unchecked

        assert(store.delete(key))(isRight(equalTo(false))) &&
        assert(store.put(key, Array[Byte](1)))(isRight(anything)) &&
        assert(store.delete(key))(isRight(equalTo(true))) &&
        assert(store.delete(key))(isRight(equalTo(false)))
      },
      test("list returns keys with a prefix in lexicographic order") {
        val store     = BlobStore.inMemory
        val Right(a1) = BlobKey.either("a/1"): @unchecked
        val Right(a2) = BlobKey.either("a/2"): @unchecked
        val Right(b1) = BlobKey.either("b/1"): @unchecked

        store.put(a2, Array[Byte](2))
        store.put(b1, Array[Byte](3))
        store.put(a1, Array[Byte](1))

        val Right(prefix) = BlobKeyPrefix.either("a/"): @unchecked

        assert(store.list(prefix))(isRight(equalTo(Vector(a1, a2))))
      },
      test("BlobKey validation rejects empty and absolute keys") {
        assert(BlobKey.fromString("").toEither)(
          isLeft(
            hasField(
              "head",
              (_: NonEmptyChunk[BlobStoreError]).head,
              hasField("message", (_: BlobStoreError).message, containsString("empty"))
            )
          )
        ) &&
        assert(BlobKey.fromString("/abs").toEither)(
          isLeft(
            hasField(
              "head",
              (_: NonEmptyChunk[BlobStoreError]).head,
              hasField("message", (_: BlobStoreError).message, containsString("start with '/'"))
            )
          )
        )
      }
    )
}
