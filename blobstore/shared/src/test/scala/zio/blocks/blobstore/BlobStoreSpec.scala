package zio.blocks.blobstore

import zio.test._
import zio.test.Assertion._

object BlobStoreSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("BlobStoreSpec")(
      test("put/get round-trips bytes") {
        val store = BlobStore.inMemory
        val key   = BlobKey.unsafeFromString("a/b/c.bin")
        val bytes = Array[Byte](1, 2, 3, 4)

        assert(store.put(key, bytes))(isRight(hasField("size", (_: BlobMetadata).size, equalTo(4L)))) &&
        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(bytes.toVector))))
      },
      test("put defensively copies the input array") {
        val store = BlobStore.inMemory
        val key   = BlobKey.unsafeFromString("x")
        val bytes = Array[Byte](1, 2, 3)

        store.put(key, bytes)
        bytes(0) = 9

        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(Array[Byte](1, 2, 3).toVector))))
      },
      test("get defensively copies the stored array") {
        val store = BlobStore.inMemory
        val key   = BlobKey.unsafeFromString("x")

        store.put(key, Array[Byte](1, 2, 3))
        val Right(Some(out)) = store.get(key): @unchecked
        out(0) = 9

        assert(store.get(key).map(_.map(_.toVector)))(isRight(isSome(equalTo(Array[Byte](1, 2, 3).toVector))))
      },
      test("head returns metadata without content") {
        val store = BlobStore.inMemory
        val key   = BlobKey.unsafeFromString("k")

        store.put(key, Array.fill[Byte](10)(42))

        assert(store.head(key))(isRight(isSome(equalTo(BlobMetadata(key, 10L)))))
      },
      test("delete returns whether the key existed") {
        val store = BlobStore.inMemory
        val key   = BlobKey.unsafeFromString("k")

        assert(store.delete(key))(isRight(equalTo(false))) &&
        assert(store.put(key, Array[Byte](1)))(isRight(anything)) &&
        assert(store.delete(key))(isRight(equalTo(true))) &&
        assert(store.delete(key))(isRight(equalTo(false)))
      },
      test("list returns keys with a prefix in lexicographic order") {
        val store = BlobStore.inMemory
        val a1    = BlobKey.unsafeFromString("a/1")
        val a2    = BlobKey.unsafeFromString("a/2")
        val b1    = BlobKey.unsafeFromString("b/1")

        store.put(a2, Array[Byte](2))
        store.put(b1, Array[Byte](3))
        store.put(a1, Array[Byte](1))

        val Right(prefix) = BlobKeyPrefix.fromString("a/"): @unchecked

        assert(store.list(prefix))(isRight(equalTo(Vector(a1, a2))))
      },
      test("BlobKey validation rejects empty and absolute keys") {
        assert(BlobKey.fromString(""))(isLeft(hasField("message", (_: BlobStoreError).message, containsString("empty")))) &&
        assert(BlobKey.fromString("/abs"))(
          isLeft(hasField("message", (_: BlobStoreError).message, containsString("start with '/'")))
        )
      }
    )
}

