package zio.blocks.schema

import zio.blocks.schema.binding.Binding

import java.util.concurrent.ConcurrentHashMap

/**
 * A {{Schema}} is a data type that contains reified information on the
 * structure of a Scala data type, together with the ability to tear down and
 * build up values of that type.
 */
final case class Schema[A](reflect: Reflect.Bound[A]) {
  private val cache: ConcurrentHashMap[codec.Format, _] = new ConcurrentHashMap

  private def getInstance[F <: codec.Format](format: F): format.TypeClass[A] =
    cache
      .asInstanceOf[ConcurrentHashMap[codec.Format, format.TypeClass[A]]]
      .computeIfAbsent(format, _ => derive(format))

  def defaultValue[B](optic: Optic.Bound[A, B], value: => B): Schema[A] = ??? // TODO

  def defaultValue(value: => A): Schema[A] = ??? // TODO

  def derive[F <: codec.Format](format: F): format.TypeClass[A] = ??? // TODO

  def deriving[F <: codec.Format](format: F): DerivationBuilder[format.TypeClass, A] = ??? // TODO

  def decode[F <: codec.Format](format: F)(decodeInput: format.DecodeInput): Either[codec.CodecError, A] =
    getInstance(format).decode(decodeInput)

  def doc: Doc = reflect.doc

  def doc(value: String): Schema[A] = ??? // TODO

  def doc[B](optic: Optic.Bound[A, B]): Doc = optic.focus.doc

  def doc[B](optic: Optic.Bound[A, B])(value: String): Schema[A] = ??? // TODO

  def encode[F <: codec.Format](format: F)(output: format.EncodeOutput)(value: A): Unit =
    getInstance(format).encode(value, output)

  def examples: List[A] = reflect.binding.examples

  def examples(value: A, values: A*): Schema[A] = ??? // TODO

  def examples[B](optic: Optic.Bound[A, B]): List[B] = optic.focus.binding.examples

  def examples[B](optic: Optic.Bound[A, B])(value: B, values: B*): Schema[A] = ??? // TODO

  def fromDynamicValue(value: DynamicValue): Either[codec.CodecError, A] = ??? // TODO

  def toDynamicValue(value: A): DynamicValue = ??? // TODO
}
object Schema {

  def apply[A](implicit schema: Schema[A]): Schema[A] = schema

  implicit val unit: Schema[Unit] = Schema(Reflect.unit[Binding])

  implicit val boolean: Schema[Boolean] = Schema(Reflect.boolean[Binding])

  implicit val byte: Schema[Byte] = Schema(Reflect.byte[Binding])

  implicit val short: Schema[Short] = Schema(Reflect.short[Binding])

  implicit val int: Schema[Int] = Schema(Reflect.int[Binding])

  implicit val long: Schema[Long] = Schema(Reflect.long[Binding])

  implicit val float: Schema[Float] = Schema(Reflect.float[Binding])

  implicit val double: Schema[Double] = Schema(Reflect.double[Binding])

  implicit val char: Schema[Char] = Schema(Reflect.char[Binding])

  implicit val string: Schema[String] = Schema(Reflect.string[Binding])

  implicit val bigInteger: Schema[BigInt] = Schema(Reflect.bigInt[Binding])

  implicit val bigDecimal: Schema[BigDecimal] = Schema(Reflect.bigDecimal[Binding])

  implicit val dayOfWeek: Schema[java.time.DayOfWeek] = Schema(Reflect.dayOfWeek[Binding])

  implicit val duration: Schema[java.time.Duration] = Schema(Reflect.duration[Binding])

  implicit val instant: Schema[java.time.Instant] = Schema(Reflect.instant[Binding])

  implicit val localDate: Schema[java.time.LocalDate] = Schema(Reflect.localDate[Binding])

  implicit val localDateTime: Schema[java.time.LocalDateTime] = Schema(Reflect.localDateTime[Binding])

  implicit val localTime: Schema[java.time.LocalTime] = Schema(Reflect.localTime[Binding])

  implicit val month: Schema[java.time.Month] = Schema(Reflect.month[Binding])

  implicit val monthDay: Schema[java.time.MonthDay] = Schema(Reflect.monthDay[Binding])

  implicit val offsetDateTime: Schema[java.time.OffsetDateTime] = Schema(Reflect.offsetDateTime[Binding])

  implicit val offsetTime: Schema[java.time.OffsetTime] = Schema(Reflect.offsetTime[Binding])

  implicit val period: Schema[java.time.Period] = Schema(Reflect.period[Binding])

  implicit val year: Schema[java.time.Year] = Schema(Reflect.year[Binding])

  implicit val yearMonth: Schema[java.time.YearMonth] = Schema(Reflect.yearMonth[Binding])

  implicit val zoneId: Schema[java.time.ZoneId] = Schema(Reflect.zoneId[Binding])

  implicit val zoneOffset: Schema[java.time.ZoneOffset] = Schema(Reflect.zoneOffset[Binding])

  implicit val zonedDateTime: Schema[java.time.ZonedDateTime] = Schema(Reflect.zonedDateTime[Binding])

  implicit val currency: Schema[java.util.Currency] = Schema(Reflect.currency[Binding])

  implicit val uuid: Schema[java.util.UUID] = Schema(Reflect.uuid[Binding])

  implicit def set[A](implicit element: Schema[A]): Schema[Set[A]] = Schema(Reflect.set(element.reflect))

  implicit def list[A](implicit element: Schema[A]): Schema[List[A]] = Schema(Reflect.list(element.reflect))

  implicit def vector[A](implicit element: Schema[A]): Schema[Vector[A]] = Schema(Reflect.vector(element.reflect))

  implicit def array[A](implicit element: Schema[A]): Schema[Array[A]] = Schema(Reflect.array(element.reflect))

  implicit def map[A, B](implicit key: Schema[A], value: Schema[B]): Schema[collection.immutable.Map[A, B]] =
    Schema(Reflect.map(key.reflect, value.reflect))

  implicit def some[A](implicit element: Schema[A]): Schema[Some[A]] = Schema(Reflect.some(element.reflect))

  implicit val none: Schema[None.type] = Schema(Reflect.none[Binding])

  implicit def option[A](implicit element: Schema[A]): Schema[Option[A]] = Schema(Reflect.option(element.reflect))

  implicit def left[A, B](implicit element: Schema[A]): Schema[Left[A, B]] = Schema(
    Reflect.left[Binding, A, B](element.reflect)
  )

  implicit def right[A, B](implicit element: Schema[B]): Schema[Right[A, B]] = Schema(
    Reflect.right[Binding, A, B](element.reflect)
  )

  implicit def either[L, R](implicit l: Schema[L], r: Schema[R]): Reflect.Bound[Either[L, R]] =
    Reflect.either(l.reflect, r.reflect)

  implicit def tuple2[A, B](implicit a: Schema[A], b: Schema[B]): Reflect.Bound[(A, B)] =
    Reflect.tuple2(a.reflect, b.reflect)

  implicit def tuple3[A, B, C](implicit a: Schema[A], b: Schema[B], c: Schema[C]): Reflect.Bound[(A, B, C)] =
    Reflect.tuple3(a.reflect, b.reflect, c.reflect)

  implicit def tuple4[A, B, C, D](implicit
    a: Schema[A],
    b: Schema[B],
    c: Schema[C],
    d: Schema[D]
  ): Reflect.Bound[(A, B, C, D)] = Reflect.tuple4(a.reflect, b.reflect, c.reflect, d.reflect)

  implicit def tuple5[A, B, C, D, E](implicit
    a: Schema[A],
    b: Schema[B],
    c: Schema[C],
    d: Schema[D],
    e: Schema[E]
  ): Reflect.Bound[(A, B, C, D, E)] = Reflect.tuple5(a.reflect, b.reflect, c.reflect, d.reflect, e.reflect)
}
