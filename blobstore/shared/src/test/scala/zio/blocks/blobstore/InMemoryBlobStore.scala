package zio.blocks.blobstore

import scala.collection.immutable.ArraySeq

/**
 * Test-only in-memory implementation.
 *
 * Kept in `src/test` so it is not published and cannot be used from production
 * code.
 */
final class InMemoryBlobStore() extends BlobStore {
  private[this] val lock: AnyRef                     = new AnyRef
  private[this] var state: Map[BlobKey, Array[Byte]] = Map.empty

  override def put(key: BlobKey, bytes: Array[Byte]): Either[BlobStoreError, BlobMetadata] =
    if (bytes eq null) Left(BlobStoreError.Unexpected("Bytes must not be null"))
    else {
      val copy = bytes.clone()
      lock.synchronized {
        state = state.updated(key, copy)
      }
      Right(BlobMetadata(key, copy.length.toLong))
    }

  override def get(key: BlobKey): Either[BlobStoreError, Option[Array[Byte]]] =
    lock.synchronized {
      Right(state.get(key).map(_.clone()))
    }

  override def head(key: BlobKey): Either[BlobStoreError, Option[BlobMetadata]] =
    lock.synchronized {
      Right(state.get(key).map(bytes => BlobMetadata(key, bytes.length.toLong)))
    }

  override def delete(key: BlobKey): Either[BlobStoreError, Boolean] =
    lock.synchronized {
      val existed = state.contains(key)
      if (existed) state = state - key
      Right(existed)
    }

  override def list(prefix: BlobKeyPrefix): Either[BlobStoreError, Vector[BlobKey]] =
    lock.synchronized {
      val keys =
        if (prefix.value.isEmpty) state.keysIterator
        else state.keysIterator.filter(_.value.startsWith(prefix.value))

      Right(keys.toArray.sorted.to(ArraySeq).toVector)
    }
}
