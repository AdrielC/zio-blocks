package zio.blocks.schema

import zio.blocks.schema.binding.{Binding, NoBinding}
import zio.test._

object ReflectJsonSchemaSpec extends ZIOSpecDefault {

  final case class Person(name: String, age: Int)

  sealed trait Animal
  object Animal {
    final case class Dog(age: Int) extends Animal
    final case class Cat(name: String) extends Animal
  }

  final case class Age(value: Int)

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

  private def roundTrip[A](schema: Reflect[NoBinding, A]) = {
    val json    = schema.toJsonSchema.toJson
    val decoded = Reflect.fromJsonSchema(DynamicValue.fromJson(json)).map(_.asInstanceOf[Reflect[NoBinding, A]])
    assertTrue(decoded == Right(schema))
  }

  def spec =
    suite("ReflectJsonSchemaSpec")(
      test("primitive round-trip") {
        val s = Reflect.int[Binding].noBinding
        roundTrip(s)
      },
      test("record round-trip") {
        roundTrip(personSchema)
      },
      test("variant round-trip") {
        val dogSchema: Reflect[NoBinding, Animal.Dog] =
          Reflect.Record(
            Vector(Term("age", Reflect.int[Binding].noBinding)),
            TypeName(Namespace(List("example")), "Dog"),
            NoBinding()
          )
        val catSchema: Reflect[NoBinding, Animal.Cat] =
          Reflect.Record(
            Vector(Term("name", Reflect.string[Binding].noBinding)),
            TypeName(Namespace(List("example")), "Cat"),
            NoBinding()
          )
        val animalSchema: Reflect[NoBinding, Animal] =
          Reflect.Variant(
            Vector(
              Term("Dog", dogSchema.asInstanceOf[Reflect[NoBinding, Animal.Dog]]),
              Term("Cat", catSchema.asInstanceOf[Reflect[NoBinding, Animal.Cat]])
            ),
            TypeName(Namespace(List("example")), "Animal"),
            NoBinding()
          )
        roundTrip(animalSchema)
      },
      test("sequence round-trip") {
        val seqSchema: Reflect[NoBinding, List[Int]] =
          Reflect.Sequence(
            Reflect.int[Binding].noBinding.asInstanceOf[Reflect[NoBinding, Int]],
            TypeName.unit.asInstanceOf[TypeName[List[Int]]],
            NoBinding()
          )
        roundTrip(seqSchema)
      },
      test("map round-trip") {
        val mapSchema: Reflect[NoBinding, scala.collection.immutable.Map[String, Int]] =
          Reflect.Map(
            Reflect.string[Binding].noBinding.asInstanceOf[Reflect[NoBinding, String]],
            Reflect.int[Binding].noBinding.asInstanceOf[Reflect[NoBinding, Int]],
            TypeName.unit
              .asInstanceOf[TypeName[scala.collection.immutable.Map[String, Int]]],
            NoBinding()
          )
        roundTrip(mapSchema)
      },
      test("dynamic round-trip") {
        val dynamicSchema: Reflect[NoBinding, DynamicValue] = Reflect.Dynamic(NoBinding())
        roundTrip(dynamicSchema)
      },
      test("wrapper round-trip") {
        val wrapperSchema: Reflect[NoBinding, Age] =
          Reflect.Wrapper(
            Reflect.int[Binding].noBinding.asInstanceOf[Reflect[NoBinding, Int]],
            TypeName(Namespace(List("example")), "Age"),
            NoBinding()
          )
        roundTrip(wrapperSchema)
      }
    )
}

