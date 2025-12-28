package zio.blocks.blobstore

import scala.collection.immutable.ArraySeq
import scala.util.control.NoStackTrace

/**
 * A minimal, dependency-free blob store API.
 *
 * This API is intentionally synchronous and side-effecting (it does not depend on ZIO/Cats Effect).
 * Downstream users can wrap these operations in their effect system of choice.
 */
trait BlobStore {

  /**
   * Stores the given bytes under the provided key.
   *
   * Implementations should defensively copy the input bytes.
   */
  def put(key: BlobKey, bytes: Array[Byte]): Either[BlobStoreError, BlobMetadata]

  /**
   * Retrieves the bytes stored under the provided key.
   *
   * Implementations should defensively copy the stored bytes before returning.
   */
  def get(key: BlobKey): Either[BlobStoreError, Option[Array[Byte]]]

  /** Retrieves metadata (if present) without returning the blob content. */
  def head(key: BlobKey): Either[BlobStoreError, Option[BlobMetadata]]

  /** Deletes the blob if present; returns true if something was deleted. */
  def delete(key: BlobKey): Either[BlobStoreError, Boolean]

  /** Lists blob keys under the specified prefix, in lexicographic order. */
  def list(prefix: BlobKeyPrefix): Either[BlobStoreError, Vector[BlobKey]]
}

object BlobStore {

  /** A simple in-memory implementation, useful for tests. */
  def inMemory: BlobStore = new InMemoryBlobStore()
}

final case class BlobMetadata(key: BlobKey, size: Long)

/**
 * A validated blob key.
 *
 * Invariants:
 * - non-empty
 * - does not start with '/'
 * - does not contain NUL
 */
final case class BlobKey private (value: String) extends AnyVal {
  override def toString: String = value
}

object BlobKey {
  def fromString(value: String): Either[BlobStoreError, BlobKey] =
    if (value eq null) Left(BlobStoreError.InvalidKey("Key must not be null"))
    else if (value.isEmpty) Left(BlobStoreError.InvalidKey("Key must not be empty"))
    else if (value.charAt(0) == '/') Left(BlobStoreError.InvalidKey("Key must not start with '/'"))
    else if (value.indexOf('\u0000') >= 0) Left(BlobStoreError.InvalidKey("Key must not contain NUL"))
    else Right(new BlobKey(value))

  def unsafeFromString(value: String): BlobKey =
    fromString(value) match {
      case Right(k) => k
      case Left(e)  => throw e
    }
}

/**
 * A validated prefix. Empty prefix means "everything".
 *
 * Invariants:
 * - not null
 * - does not start with '/'
 * - does not contain NUL
 */
final case class BlobKeyPrefix private (value: String) extends AnyVal {
  override def toString: String = value
}

object BlobKeyPrefix {
  val empty: BlobKeyPrefix = new BlobKeyPrefix("")

  def fromString(value: String): Either[BlobStoreError, BlobKeyPrefix] =
    if (value eq null) Left(BlobStoreError.InvalidPrefix("Prefix must not be null"))
    else if (value.nonEmpty && value.charAt(0) == '/') Left(BlobStoreError.InvalidPrefix("Prefix must not start with '/'"))
    else if (value.indexOf('\u0000') >= 0) Left(BlobStoreError.InvalidPrefix("Prefix must not contain NUL"))
    else Right(new BlobKeyPrefix(value))
}

sealed trait BlobStoreError extends Exception with NoStackTrace {
  def message: String

  override final def getMessage: String = message
}

object BlobStoreError {
  final case class InvalidKey(message: String) extends BlobStoreError
  final case class InvalidPrefix(message: String) extends BlobStoreError
  final case class StorageCorruption(message: String) extends BlobStoreError
  final case class Unexpected(message: String) extends BlobStoreError
}

final class InMemoryBlobStore() extends BlobStore {
  private[this] val lock: AnyRef = new AnyRef
  private[this] var state: Map[String, Array[Byte]] = Map.empty

  override def put(key: BlobKey, bytes: Array[Byte]): Either[BlobStoreError, BlobMetadata] =
    if (bytes eq null) Left(BlobStoreError.Unexpected("Bytes must not be null"))
    else {
      val copy = bytes.clone()
      lock.synchronized {
        state = state.updated(key.value, copy)
      }
      Right(BlobMetadata(key, copy.length.toLong))
    }

  override def get(key: BlobKey): Either[BlobStoreError, Option[Array[Byte]]] =
    lock.synchronized {
      Right(state.get(key.value).map(_.clone()))
    }

  override def head(key: BlobKey): Either[BlobStoreError, Option[BlobMetadata]] =
    lock.synchronized {
      Right(state.get(key.value).map(bytes => BlobMetadata(key, bytes.length.toLong)))
    }

  override def delete(key: BlobKey): Either[BlobStoreError, Boolean] =
    lock.synchronized {
      val existed = state.contains(key.value)
      if (existed) state = state - key.value
      Right(existed)
    }

  override def list(prefix: BlobKeyPrefix): Either[BlobStoreError, Vector[BlobKey]] =
    lock.synchronized {
      val keys =
        if (prefix.value.isEmpty) state.keysIterator
        else state.keysIterator.filter(_.startsWith(prefix.value))

      Right(
        keys.toArray
          .sorted
          .iterator
          .map(s => new BlobKey(s))
          .to(ArraySeq)
          .toVector
      )
    }
}

