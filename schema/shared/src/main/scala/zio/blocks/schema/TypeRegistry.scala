package zio.blocks.schema

import zio.blocks.schema.binding._
import scala.collection.immutable.Map

/**
 * A TypeRegistry is a type-safe map from TypeName[A] to Binding[?, A]. This
 * allows you to look up the binding for any specified type name.
 *
 * For example, you could look up the binding for `myapp.Person` data type,
 * where you would discover it is a `Binding.Record`, which allows you to
 * construct and deconstruct values of type `Person`.
 */
trait TypeRegistry {
  def lookup[A](typeName: TypeName[A]): Option[Binding[?, A]]
}

object TypeRegistry {

  /**
   * Empty type registry with no bindings
   */
  val empty: TypeRegistry = Collection.empty

  /**
   * Default type registry with bindings for all standard Scala types
   */
  lazy val default: TypeRegistry = {
    val registry = Collection.empty
      .add(TypeName.unit, Binding.Primitive.unit)
      .add(TypeName.boolean, Binding.Primitive.boolean)
      .add(TypeName.byte, Binding.Primitive.byte)
      .add(TypeName.short, Binding.Primitive.short)
      .add(TypeName.int, Binding.Primitive.int)
      .add(TypeName.long, Binding.Primitive.long)
      .add(TypeName.float, Binding.Primitive.float)
      .add(TypeName.double, Binding.Primitive.double)
      .add(TypeName.char, Binding.Primitive.char)
      .add(TypeName.string, Binding.Primitive.string)
      .add(TypeName.bigInt, Binding.Primitive.bigInt)
      .add(TypeName.bigDecimal, Binding.Primitive.bigDecimal)
      .add(TypeName.dayOfWeek, Binding.Primitive.dayOfWeek)
      .add(TypeName.duration, Binding.Primitive.duration)
      .add(TypeName.instant, Binding.Primitive.instant)
      .add(TypeName.localDate, Binding.Primitive.localDate)
      .add(TypeName.localDateTime, Binding.Primitive.localDateTime)
      .add(TypeName.localTime, Binding.Primitive.localTime)
      .add(TypeName.month, Binding.Primitive.month)
      .add(TypeName.monthDay, Binding.Primitive.monthDay)
      .add(TypeName.offsetDateTime, Binding.Primitive.offsetDateTime)
      .add(TypeName.offsetTime, Binding.Primitive.offsetTime)
      .add(TypeName.period, Binding.Primitive.period)
      .add(TypeName.year, Binding.Primitive.year)
      .add(TypeName.yearMonth, Binding.Primitive.yearMonth)
      .add(TypeName.zoneId, Binding.Primitive.zoneId)
      .add(TypeName.zoneOffset, Binding.Primitive.zoneOffset)
      .add(TypeName.zonedDateTime, Binding.Primitive.zonedDateTime)
      .add(TypeName.currency, Binding.Primitive.currency)
      .add(TypeName.uuid, Binding.Primitive.uuid)
    registry
  }

  /**
   * Collection-based implementation of TypeRegistry with add/remove methods
   */
  final case class Collection private (
    private val bindings: Map[String, (TypeName[?], Binding[?, ?])]
  ) extends TypeRegistry {

    def lookup[A](typeName: TypeName[A]): Option[Binding[?, A]] = {
      val key = typeNameToKey(typeName)
      bindings.get(key).map(_._2.asInstanceOf[Binding[?, A]])
    }

    def add[A](typeName: TypeName[A], binding: Binding[?, A]): Collection = {
      val key = typeNameToKey(typeName)
      Collection(bindings + (key -> (typeName, binding)))
    }

    def remove[A](typeName: TypeName[A]): Collection = {
      val key = typeNameToKey(typeName)
      Collection(bindings - key)
    }

    def contains[A](typeName: TypeName[A]): Boolean = {
      val key = typeNameToKey(typeName)
      bindings.contains(key)
    }

    def size: Int = bindings.size

    def isEmpty: Boolean = bindings.isEmpty

    def nonEmpty: Boolean = bindings.nonEmpty

    def typeNames: Seq[TypeName[?]] = bindings.values.map(_._1).toSeq

    def clear: Collection = Collection(Map.empty[String, (TypeName[?], Binding[?, ?])])

    private def typeNameToKey(typeName: TypeName[?]): String = {
      val namespace = typeName.namespace.elements.mkString(".")
      val params    = typeName.params.map(typeNameToKey).mkString("[", ",", "]")
      val paramStr  = if (typeName.params.nonEmpty) params else ""
      s"${if (namespace.nonEmpty) namespace + "." else ""}${typeName.name}$paramStr"
    }
  }

  object Collection {
    val empty: Collection = Collection(Map.empty[String, (TypeName[?], Binding[?, ?])])

    def apply[A](typeName: TypeName[A], binding: Binding[?, A]): Collection =
      empty.add(typeName, binding)

    def apply(bindings: (TypeName[?], Binding[?, ?])*): Collection =
      bindings.foldLeft(empty) { case (acc, (tn, b)) =>
        acc.add(tn.asInstanceOf[TypeName[Any]], b.asInstanceOf[Binding[?, Any]])
      }

    def fromMap(bindings: Map[TypeName[?], Binding[?, ?]]): Collection = {
      val mappedBindings = bindings.map { case (typeName, binding) =>
        val key = {
          val namespace = typeName.namespace.elements.mkString(".")
          val params    = typeName.params.map { tn =>
            val ns = tn.namespace.elements.mkString(".")
            s"${if (ns.nonEmpty) ns + "." else ""}${tn.name}"
          }.mkString("[", ",", "]")
          val paramStr = if (typeName.params.nonEmpty) params else ""
          s"${if (namespace.nonEmpty) namespace + "." else ""}${typeName.name}$paramStr"
        }
        key -> (typeName, binding)
      }
      Collection(mappedBindings)
    }
  }
}

/**
 * Error that occurs during rebinding when a type is not found in the registry
 */
sealed trait RebindError extends Product with Serializable

object RebindError {
  final case class TypeNotFound(typeName: TypeName[?])                                          extends RebindError
  final case class IncompatibleBinding(typeName: TypeName[?], expected: String, actual: String) extends RebindError
  final case class NestedError(typeName: TypeName[?], cause: RebindError)                       extends RebindError
}
