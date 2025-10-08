package zio.blocks.schema

import scala.collection.immutable.ArraySeq
import zio.blocks.schema.PrimitiveValue
import zio.blocks.schema.binding.{Binding, BindingType, NoBinding}
import zio.blocks.schema.{Namespace, TypeName}
import zio.blocks.schema.json.{JsonSchema, JsonSchemaConfig}
import zio.test.Assertion._
import zio.test._

object ReflectJsonSchemaSpec extends ZIOSpecDefault {

  final case class Person(name: String, age: Int)

  sealed trait Animal
  object Animal {
    final case class Dog(age: Int)     extends Animal
    final case class Cat(name: String) extends Animal
  }

  final case class Age(value: Int)

  private val config           = JsonSchemaConfig.default
  private val exampleNamespace = Namespace(List("example"))

  private val intSchema: Reflect[NoBinding, Int]       = Reflect.int[Binding].noBinding
  private val stringSchema: Reflect[NoBinding, String] = Reflect.string[Binding].noBinding

  private val personSchema: Reflect[NoBinding, Person] = {
    val nameField: Term[NoBinding, Person, String] =
      Term("name", stringSchema)
    val ageField: Term[NoBinding, Person, Int] =
      Term("age", intSchema)

    Reflect.Record(
      Vector(nameField, ageField),
      TypeName(exampleNamespace, "Person"),
      NoBinding()
    )
  }

  private val dogSchema: Reflect[NoBinding, Animal.Dog] = {
    val ageField: Term[NoBinding, Animal.Dog, Int] =
      Term("age", intSchema)

    Reflect.Record(
      Vector(ageField),
      TypeName(exampleNamespace, "Dog"),
      NoBinding()
    )
  }

  private val catSchema: Reflect[NoBinding, Animal.Cat] = {
    val nameField: Term[NoBinding, Animal.Cat, String] =
      Term("name", stringSchema)

    Reflect.Record(
      Vector(nameField),
      TypeName(exampleNamespace, "Cat"),
      NoBinding()
    )
  }

  private val animalSchema: Reflect[NoBinding, Animal] = {
    val dogCase: Term[NoBinding, Animal, Animal.Dog] =
      Term("Dog", dogSchema)
    val catCase: Term[NoBinding, Animal, Animal.Cat] =
      Term("Cat", catSchema)

    Reflect.Variant(
      Vector(dogCase, catCase).asInstanceOf[IndexedSeq[Term[NoBinding, Animal, ? <: Animal]]],
      TypeName(exampleNamespace, "Animal"),
      NoBinding()
    )
  }

  private val listOfIntSchema: Reflect[NoBinding, List[Int]] =
    Reflect.Sequence(
      intSchema,
      TypeName(Namespace(List("scala", "collection", "immutable")), "List"),
      NoBinding()
    )

  private val mapSchema: Reflect[NoBinding, Map[String, Int]] =
    Reflect.Map(
      stringSchema,
      intSchema,
      TypeName(Namespace(List("scala", "collection", "immutable")), "Map"),
      NoBinding()
    )

  private val ageWrapperSchema: Reflect[NoBinding, Age] =
    Reflect.Wrapper(
      intSchema,
      TypeName(exampleNamespace, "Age"),
      NoBinding[BindingType.Wrapper[Age, Int], Age]()
    )

  private val personSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"              -> dvString("object"),
        config.nodeTypeKey   -> dvString("record"),
        config.typeNameKey   -> dvString("example.Person"),
        "properties"        -> DynamicValue.Record(
          ArraySeq(
            "name" -> DynamicValue.Record(ArraySeq("type" -> dvString("string"))),
            "age"  -> DynamicValue.Record(ArraySeq("type" -> dvString("integer")))
          )
        ),
        "required"          -> DynamicValue.Sequence(ArraySeq(dvString("name"), dvString("age")))
      )
    )

  private val dogSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"              -> dvString("object"),
        config.nodeTypeKey   -> dvString("record"),
        config.typeNameKey   -> dvString("example.Dog"),
        "properties"        -> DynamicValue.Record(
          ArraySeq(
            "age" -> DynamicValue.Record(ArraySeq("type" -> dvString("integer")))
          )
        ),
        "required"          -> DynamicValue.Sequence(ArraySeq(dvString("age")))
      )
    )

  private val catSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"              -> dvString("object"),
        config.nodeTypeKey   -> dvString("record"),
        config.typeNameKey   -> dvString("example.Cat"),
        "properties"        -> DynamicValue.Record(
          ArraySeq(
            "name" -> DynamicValue.Record(ArraySeq("type" -> dvString("string")))
          )
        ),
        "required"          -> DynamicValue.Sequence(ArraySeq(dvString("name")))
      )
    )

  private def variantAlternative(caseName: String, valueSchema: DynamicValue): DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"       -> dvString("object"),
        "properties" -> DynamicValue.Record(
          ArraySeq(
            "tag"   -> DynamicValue.Record(ArraySeq("const" -> dvString(caseName))),
            "value" -> valueSchema
          )
        ),
        "required"   -> DynamicValue.Sequence(ArraySeq(dvString("tag"), dvString("value")))
      )
    )

  private val animalSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        config.nodeTypeKey -> dvString("variant"),
        config.typeNameKey -> dvString("example.Animal"),
        "oneOf"           -> DynamicValue.Sequence(
          ArraySeq(
            variantAlternative("Dog", dogSchemaJson),
            variantAlternative("Cat", catSchemaJson)
          )
        )
      )
    )

  private val listOfIntSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"            -> dvString("array"),
        "items"           -> DynamicValue.Record(ArraySeq("type" -> dvString("integer"))),
        config.nodeTypeKey -> dvString("sequence"),
        config.typeNameKey -> dvString("scala.collection.immutable.List")
      )
    )

  private val mapSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        "type"                   -> dvString("object"),
        "additionalProperties"   -> DynamicValue.Record(ArraySeq("type" -> dvString("integer"))),
        config.nodeTypeKey        -> dvString("map"),
        config.typeNameKey        -> dvString("scala.collection.immutable.Map"),
        config.keyKey             -> DynamicValue.Record(ArraySeq("type" -> dvString("string")))
      )
    )

  private val ageWrapperSchemaJson: DynamicValue =
    DynamicValue.Record(
      ArraySeq(
        config.nodeTypeKey -> dvString("wrapper"),
        config.typeNameKey -> dvString("example.Age"),
        config.wrappedKey  -> DynamicValue.Record(ArraySeq("type" -> dvString("integer")))
      )
    )

  private def dvString(value: String): DynamicValue =
    DynamicValue.Primitive(PrimitiveValue.String(value))

  def spec =
    suite("ReflectJsonSchemaSpec")(
      test("recursive schema emits $defs and $ref") {

        final case class Node(next: Option[Node])

        val nodeType: TypeName[Node] = TypeName(Namespace(List("example")), "Node")

        lazy val node: Reflect[NoBinding, Node] = Reflect.Deferred(() =>
          Reflect.Record(
            Vector(
              Term("next", Reflect.Deferred(() => node))
            ),
            nodeType,
            NoBinding()
          )
        )

        val _    = Node(None)
        val dv   = node.toJsonSchema
        val json = dv.toJson
        assertTrue(json.contains("\"$defs\"")) && assertTrue(json.contains("\"$ref\""))
      },
      test("decodes primitive schema") {
        val primitiveSchema = DynamicValue.Record(ArraySeq("type" -> dvString("integer")))
        val expected        = Reflect.int[Binding].noBinding.asInstanceOf[Reflect[NoBinding, Int]]
        val result          = JsonSchema.fromJsonSchema(primitiveSchema)

        assert(result)(isRight(equalTo(expected)))
      },
      test("decodes record schema produced by Reflect") {
        val result = JsonSchema.fromJsonSchema(personSchemaJson)

        assert(result)(isRight(equalTo(personSchema)))
      },
      test("decodes variant schema produced by Reflect") {
        val result = JsonSchema.fromJsonSchema(animalSchemaJson)

        assert(result)(isRight(equalTo(animalSchema)))
      },
      test("decodes sequence schema produced by Reflect") {
        val result = JsonSchema.fromJsonSchema(listOfIntSchemaJson)

        assert(result)(isRight(equalTo(listOfIntSchema)))
      },
      test("decodes map schema produced by Reflect") {
        val result = JsonSchema.fromJsonSchema(mapSchemaJson)

        assert(result)(isRight(equalTo(mapSchema)))
      },
      test("decodes wrapper schema produced by Reflect") {
        val result = JsonSchema.fromJsonSchema(ageWrapperSchemaJson)

        assert(result)(isRight(equalTo(ageWrapperSchema)))
      },
      test("array schema without items fails with SchemaError") {
        val arraySchema = DynamicValue.Record(ArraySeq("type" -> dvString("array")))
        val result      = JsonSchema.fromJsonSchema(arraySchema)

        assert(result)(isLeft(equalTo(SchemaError.invalidType(Nil, "array items missing"))))
      },
      test("oneOf alternative without value schema fails") {
        val config      = JsonSchemaConfig.default
        val invalidAlt  = DynamicValue.Record(ArraySeq("type" -> dvString("object")))
        val variantSchema = DynamicValue.Record(
          ArraySeq(
            "oneOf"           -> DynamicValue.Sequence(ArraySeq(invalidAlt)),
            config.nodeTypeKey -> dvString("variant")
          )
        )

        val result = JsonSchema.fromJsonSchema(variantSchema)
        assert(result)(isLeft(equalTo(SchemaError.invalidType(Nil, "invalid oneOf alternative"))))
      },
      test("wrapper schema missing wrapped field fails") {
        val config = JsonSchemaConfig.default
        val schema = DynamicValue.Record(
          ArraySeq(
            config.nodeTypeKey -> dvString("wrapper"),
            config.typeNameKey -> dvString("example.Age")
          )
        )

        val result = JsonSchema.fromJsonSchema(schema)
        assert(result)(isLeft(equalTo(SchemaError.invalidType(Nil, "wrapper missing wrapped"))))
      }
    )
}
