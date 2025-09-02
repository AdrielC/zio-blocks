package zio.blocks.schema
package json

import zio.test._
import zio.test.Assertion._

object JsonSchemaSpec extends ZIOSpecDefault {

  final case class Foo(a: Int, b: String)
  object Foo { implicit val schema: Schema[Foo] = Schema.derived }

  sealed trait Bar
  object Bar {
    final case class X(v: Int)    extends Bar
    final case class Y(s: String) extends Bar
    implicit val schemaX: Schema[X]  = Schema.derived
    implicit val schemaY: Schema[Y]  = Schema.derived
    implicit val schema: Schema[Bar] = Schema.derived
  }

  final case class Node(value: Int, next: Option[Node])
  object Node { implicit val schema: Schema[Node] = Schema.derived }

  private def fieldsOf(dv: DynamicValue): Map[String, DynamicValue] = dv match {
    case DynamicValue.Record(fields) => fields.iterator.toMap
    case _                           => Map.empty
  }

  private def hasAnyRef(dv: DynamicValue): Boolean = dv match {
    case DynamicValue.Record(fields) =>
      fields.exists(_._1 == "$ref") || fields.exists { case (_, v) => hasAnyRef(v) }
    case DynamicValue.Sequence(items) => items.exists(hasAnyRef)
    case _                            => false
  }

  def spec: Spec[TestEnvironment, Any] = suite("JsonSchemaSpec")(
    test("primitive: int -> type integer (minimal)") {
      val dv = Schema[Int].reflect.toJsonSchema(JsonSchemaConfig.minimal)
      val m  = fieldsOf(dv)
      assert(m.get("type"))(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("integer")))))
    },
    test("primitive with format: uuid -> type string, format uuid (minimal)") {
      val dv  = Schema[java.util.UUID].reflect.toJsonSchema(JsonSchemaConfig.minimal)
      val m   = fieldsOf(dv)
      val tpe = m.get("type")
      val fmt = m.get("format")
      assert(tpe)(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("string"))))) &&
      assert(fmt)(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("uuid")))))
    },
    test("record root inlines body and includes $defs when present") {
      val dv = Schema[Foo].toJsonSchema(JsonSchemaConfig.minimal)
      val m  = fieldsOf(dv)
      assert(m.get("type"))(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("object"))))) &&
      assert(m.contains("$defs"))(isTrue)
    },
    test("variant -> oneOf with tag/value shape (minimal)") {
      val dv = Schema[Bar].toJsonSchema(JsonSchemaConfig.minimal)
      val m  = fieldsOf(dv)
      assert(m.get("oneOf"))(isSome(isSubtype[DynamicValue.Sequence](anything)))
    },
    test("sequence -> array with items (minimal)") {
      val dv = Schema[List[Int]].toJsonSchema(JsonSchemaConfig.minimal)
      val m  = fieldsOf(dv)
      assert(m.get("type"))(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("array"))))) &&
      assert(m.contains("items"))(isTrue)
    },
    test("map -> object with additionalProperties referencing value (minimal)") {
      val dv = Schema[Map[String, Int]].reflect.toJsonSchema(JsonSchemaConfig.minimal)
      val m  = fieldsOf(dv)
      assert(m.get("type"))(isSome(equalTo(DynamicValue.Primitive(PrimitiveValue.String("object"))))) &&
      assert(m.contains("additionalProperties"))(isTrue)
    },
    test("recursive type does not overflow and uses $defs/$ref") {
      val dv    = Schema[Node].reflect.toJsonSchema(JsonSchemaConfig.minimal)
      val m     = fieldsOf(dv)
      val hasDf = m.contains("$defs")
      assert(hasDf)(isTrue) && assert(hasAnyRef(dv))(isTrue)
    }
  )
}
