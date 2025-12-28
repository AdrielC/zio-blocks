package zio.blocks.blobstore

import zio.prelude.ZValidation

import scala.util.control.NoStackTrace

/**
 * A minimal blob store API.
 *
 * This API is intentionally synchronous and side-effecting (it does not depend
 * on ZIO/Cats Effect). Downstream users can wrap these operations in their
 * effect system of choice.
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

object BlobStore {}

final case class BlobMetadata(key: BlobKey, size: Long)

/**
 * A validated blob key.
 *
 * Invariants:
 *   - non-empty
 *   - does not start with '/'
 *   - does not contain NUL
 */
final case class BlobKey private (value: String) extends AnyVal {
  override def toString: String = value
}

object BlobKey {
  type Validated[+A] = ZValidation[Nothing, BlobStoreError, A]

  def apply(value: String): Validated[BlobKey] =
    fromString(value)

  def fromString(value: String): Validated[BlobKey] =
    if (value eq null) ZValidation.fail(BlobStoreError.InvalidKey("Key must not be null"))
    else {
      val nonEmpty =
        ZValidation
          .fromPredicateWith(BlobStoreError.InvalidKey("Key must not be empty"))(value)((s: String) => s.nonEmpty)
      val notAbsolute =
        ZValidation
          .fromPredicateWith(BlobStoreError.InvalidKey("Key must not start with '/'"))(value)((s: String) =>
            s.isEmpty || s.charAt(0) != '/'
          )
      val noNul =
        ZValidation
          .fromPredicateWith(BlobStoreError.InvalidKey("Key must not contain NUL"))(value)((s: String) =>
            s.indexOf('\u0000') < 0
          )

      ZValidation.validateWith(nonEmpty, notAbsolute, noNul) { (_, _, _) =>
        new BlobKey(value)
      }
    }

  def either(value: String): Either[zio.NonEmptyChunk[BlobStoreError], BlobKey] =
    fromString(value).toEither

  implicit val ordering: Ordering[BlobKey] =
    Ordering.by(_.value)
}

/**
 * A validated prefix. Empty prefix means "everything".
 *
 * Invariants:
 *   - not null
 *   - does not start with '/'
 *   - does not contain NUL
 */
final case class BlobKeyPrefix private (value: String) extends AnyVal {
  override def toString: String = value
}

object BlobKeyPrefix {
  type Validated[+A] = ZValidation[Nothing, BlobStoreError, A]

  val empty: BlobKeyPrefix = new BlobKeyPrefix("")

  def apply(value: String): Validated[BlobKeyPrefix] =
    fromString(value)

  def fromString(value: String): Validated[BlobKeyPrefix] =
    if (value eq null) ZValidation.fail(BlobStoreError.InvalidPrefix("Prefix must not be null"))
    else {
      val notAbsolute =
        ZValidation
          .fromPredicateWith(BlobStoreError.InvalidPrefix("Prefix must not start with '/'"))(value)((s: String) =>
            s.isEmpty || s.charAt(0) != '/'
          )
      val noNul =
        ZValidation
          .fromPredicateWith(BlobStoreError.InvalidPrefix("Prefix must not contain NUL"))(value)((s: String) =>
            s.indexOf('\u0000') < 0
          )

      ZValidation.validateWith(notAbsolute, noNul) { (_, _) =>
        new BlobKeyPrefix(value)
      }
    }

  def either(value: String): Either[zio.NonEmptyChunk[BlobStoreError], BlobKeyPrefix] =
    fromString(value).toEither
}

sealed trait BlobStoreError extends Exception with NoStackTrace {
  def message: String

  override final def getMessage: String = message
}

object BlobStoreError {
  final case class InvalidKey(message: String)        extends BlobStoreError
  final case class InvalidPrefix(message: String)     extends BlobStoreError
  final case class StorageCorruption(message: String) extends BlobStoreError
  final case class Unexpected(message: String)        extends BlobStoreError
}
