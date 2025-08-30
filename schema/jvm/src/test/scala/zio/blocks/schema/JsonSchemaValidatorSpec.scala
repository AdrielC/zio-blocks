package zio.blocks.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.github.fge.jsonschema.main.JsonSchemaFactory
import zio.blocks.schema.binding.{Binding, NoBinding}
import zio.test._

object JsonSchemaValidatorSpec extends ZIOSpecDefault {

  final case class Person(name: String, age: Int)

  private val personSchema: Reflect[NoBinding, Person] = {
    val nameField: Term[NoBinding, Person, String] =
      Term("name", Reflect.string[Binding].noBinding)
    val ageField: Term[NoBinding, Person, Int] =
      Term("age", Reflect.int[Binding].noBinding)

    Reflect.Record(
      Vector(nameField, ageField),
      TypeName(Namespace(List("example")), "Person"),
      NoBinding()
    )
  }

  def spec =
    suite("JsonSchemaValidatorSpec")(
      test("generated schema validates sample instance") {
        val mapper      = new ObjectMapper()
        val schemaNode  = mapper.readTree(personSchema.toJsonSchema.toJson)
        val validator   = JsonSchemaFactory.byDefault().getJsonSchema(schemaNode)
        val instanceNode = JsonNodeFactory.instance.objectNode()
        instanceNode.put("name", "Alice")
        instanceNode.put("age", 30)
        val report = validator.validate(instanceNode)
        assertTrue(report.isSuccess)
      }
    )
}
