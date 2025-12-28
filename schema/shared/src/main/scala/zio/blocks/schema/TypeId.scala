package zio.blocks.schema

/**
 * Phantom-tagged type identifiers.
 *
 * At runtime, a TypeId is always just a `TypeIdRepr`.
 */
object TypeId extends TypeIdVersionSpecific {
  trait Tagged[+U] extends Any
  type @@[+T, +U] = T with Tagged[U]

  // Type-level naturals for arity.
  sealed trait Nat
  sealed trait _0 extends Nat
  sealed trait Succ[N <: Nat] extends Nat

  type _1 = Succ[_0]
  type _2 = Succ[_1]
  type _3 = Succ[_2]
  type _4 = Succ[_3]
  type _5 = Succ[_4]

  sealed trait KindTag { type Arity <: Nat }
  object KindTag {
    sealed trait Type extends KindTag { type Arity = _0 }
    sealed trait Constructor[N <: Nat] extends KindTag { type Arity = N }
  }

  type Id[+K <: KindTag] = TypeIdRepr @@ K
  type OfType            = Id[KindTag.Type]

  private[schema] def unsafeTag[K <: KindTag](repr: TypeIdRepr): Id[K] =
    repr.asInstanceOf[Id[K]]

  private[this] def ofType(repr: TypeIdRepr): OfType =
    unsafeTag[KindTag.Type](repr)

  // ---- Common type ids (kind = *) ----
  val unit: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Unit", Nil))

  val boolean: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Boolean", Nil))
  val byte: OfType    = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Byte", Nil))
  val short: OfType   = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Short", Nil))
  val int: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Int", Nil))
  val long: OfType    = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Long", Nil))
  val float: OfType   = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Float", Nil))
  val double: OfType  = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Double", Nil))
  val char: OfType    = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Char", Nil))

  val string: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "String", Nil))
  val bigInt: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "BigInt", Nil))
  val bigDecimal: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "BigDecimal", Nil))

  val dayOfWeek: OfType  = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "DayOfWeek", Nil))
  val duration: OfType   = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "Duration", Nil))
  val instant: OfType    = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "Instant", Nil))
  val localDate: OfType  = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "LocalDate", Nil))
  val localDateTime: OfType =
    ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "LocalDateTime", Nil))
  val localTime: OfType      = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "LocalTime", Nil))
  val month: OfType          = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "Month", Nil))
  val monthDay: OfType       = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "MonthDay", Nil))
  val offsetDateTime: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "OffsetDateTime", Nil))
  val offsetTime: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "OffsetTime", Nil))
  val period: OfType         = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "Period", Nil))
  val year: OfType           = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "Year", Nil))
  val yearMonth: OfType      = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "YearMonth", Nil))
  val zoneId: OfType         = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "ZoneId", Nil))
  val zoneOffset: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "ZoneOffset", Nil))
  val zonedDateTime: OfType  = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaTime), "ZonedDateTime", Nil))

  val currency: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaUtil), "Currency", Nil))
  val uuid: OfType     = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.javaUtil), "UUID", Nil))

  val none: OfType         = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "None", Nil))
  val dynamicValue: OfType = ofType(new TypeIdRepr(Owner.fromNamespace(Namespace.zioBlocksSchema), "DynamicValue", Nil))

  // Collections / constructors with applied params
  def some(element: OfType): OfType = ofType(_some.copy(params = new TypeParam(element) :: Nil))

  def option(element: OfType): OfType = ofType(_option.copy(params = new TypeParam(element) :: Nil))

  def list(element: OfType): OfType = ofType(_list.copy(params = new TypeParam(element) :: Nil))

  def map(key: OfType, value: OfType): OfType =
    ofType(_map.copy(params = new TypeParam(key) :: new TypeParam(value) :: Nil))

  def set(element: OfType): OfType = ofType(_set.copy(params = new TypeParam(element) :: Nil))

  def vector(element: OfType): OfType = ofType(_vector.copy(params = new TypeParam(element) :: Nil))

  def arraySeq(element: OfType): OfType = ofType(_arraySeq.copy(params = new TypeParam(element) :: Nil))

  def indexedSeq(element: OfType): OfType = ofType(_indexedSeq.copy(params = new TypeParam(element) :: Nil))

  def seq(element: OfType): OfType = ofType(_seq.copy(params = new TypeParam(element) :: Nil))

  private[this] val _some       = new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Some", Nil)
  private[this] val _option     = new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "Option", Nil)
  private[this] val _list       = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "List", Nil)
  private[this] val _map        = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "Map", Nil)
  private[this] val _set        = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "Set", Nil)
  private[this] val _vector     = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "Vector", Nil)
  private[this] val _arraySeq   = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "ArraySeq", Nil)
  private[this] val _indexedSeq = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "IndexedSeq", Nil)
  private[this] val _seq        = new TypeIdRepr(Owner.fromNamespace(Namespace.scalaCollectionImmutable), "Seq", Nil)
}

