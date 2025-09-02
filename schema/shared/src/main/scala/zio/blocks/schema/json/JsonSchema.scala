package zio.blocks.schema
package json

import scala.collection.immutable.{ArraySeq, ListMap}
import scala.collection.mutable
import scala.util.matching.Regex
import zio.blocks.schema.binding.{Binding, NoBinding}
import zio.blocks.schema.{PrimitiveValue => prim}
import zio.blocks.schema.DynamicValue.Record

/**
 * JsonSchema AST representation with support for bindings, validations, and
 * custom configurations.
 */
sealed trait JsonSchema[T] {
  def jsonType: String
  def title: Option[String]                                           = None
  def description: Option[String]                                     = None
  def zioValidations: Seq[ZioValidation]                              = Seq.empty
  def zioModifiers: Seq[ZioModifier]                                  = Seq.empty
  def toDynamicValue(implicit config: JsonSchemaConfig): DynamicValue =
    JsonSchema.schemaToDynamicValue(this)
}

case class ZioValidation(
  `type`: String,
  constraint: String,
  message: Option[String] = None
)

case class ZioModifier(
  `type`: String,
  key: Option[String] = None,
  value: Option[String] = None,
  description: Option[String] = None
)

/**
 * Configuration for JsonSchema generation
 */
case class JsonSchemaConfig(
  includeZioExtensions: Boolean = true,
  includeValidations: Boolean = true,
  includeBindings: Boolean = true,
  includeModifiers: Boolean = true,
  includeExamples: Boolean = true,
  includeDefaults: Boolean = true,
  namespacePrefix: String = "zio",
  validationAsComments: Boolean = false,
  validationAsGrammar: Boolean = false,
  additionalProperties: ListMap[Regex, Option[Boolean]] = ListMap.empty
) {
  val nodeTypeKey: String  = s"$namespacePrefix:nodeType"
  val typeNameKey: String  = s"$namespacePrefix:typeName"
  val primitiveKey: String = s"$namespacePrefix:primitive"
  val keyKey: String       = s"$namespacePrefix:key"
  val wrappedKey: String   = s"$namespacePrefix:wrapped"
}

object JsonSchemaConfig {
  implicit val default: JsonSchemaConfig = JsonSchemaConfig()
  val minimal: JsonSchemaConfig          = JsonSchemaConfig(
    includeZioExtensions = false,
    includeValidations = false,
    includeBindings = false,
    includeModifiers = false,
    includeExamples = false,
    includeDefaults = false,
    additionalProperties = ListMap.empty
  )
}

object JsonSchema {

  // Core JsonSchema types
  case object `boolean` extends JsonSchema[Boolean] {
    override def jsonType: String = "boolean"
  }

  case object `integer` extends JsonSchema[Int] {
    override def jsonType: String = "integer"
  }

  case object `number` extends JsonSchema[Double] {
    override def jsonType: String = "number"
  }

  case class `string`(format: Option[String] = None) extends JsonSchema[String] {
    override def jsonType: String = "string"
  }

  case class `array`[T](items: JsonSchema[T]) extends JsonSchema[List[T]] {
    override def jsonType: String = "array"
  }

  case class `object`[T](
    properties: ListMap[String, JsonSchema[?]],
    required: IndexedSeq[String] = ArraySeq.empty,
    additionalProperties: Option[Boolean] = None,
    override val title: Option[String] = None,
    override val description: Option[String] = None
  ) extends JsonSchema[T] {
    override def jsonType: String = "object"
  }

  case class `oneof`[T](alternatives: IndexedSeq[JsonSchema[? <: T]]) extends JsonSchema[T] {
    override def jsonType: String = "oneOf"
  }

  // ZIO-specific extensions
  case class `zio-primitive`[T](
    primitiveType: String,
    override val title: Option[String] = None,
    override val description: Option[String] = None,
    override val zioValidations: Seq[ZioValidation] = Seq.empty,
    override val zioModifiers: Seq[ZioModifier] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = primitiveTypeToJsonType(primitiveType)
  }

  case class `zio-record`[T](
    typeName: TypeName[T],
    fields: ListMap[String, JsonSchema[?]],
    required: IndexedSeq[String] = ArraySeq.empty,
    override val title: Option[String] = None,
    override val description: Option[String] = None,
    override val zioModifiers: Seq[ZioModifier] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = "object"
  }

  case class `zio-variant`[T](
    typeName: TypeName[T],
    cases: ListMap[String, JsonSchema[?]],
    override val title: Option[String] = None,
    override val description: Option[String] = None,
    override val zioModifiers: Seq[ZioModifier] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = "oneOf"
  }

  def fromReflect[F[_, _], A](reflect: Reflect[F, A])(implicit config: JsonSchemaConfig): JsonSchema[A] =
    reflect match {
      case r: Reflect.Primitive[F, A]   => fromPrimitive(r)
      case r: Reflect.Record[F, A]      => fromRecord(r)
      case r: Reflect.Variant[F, A]     => fromVariant(r)
      case w: Reflect.Wrapper[F, ?, ?]  => fromWrapper(w)
      case d: Reflect.Deferred[F, A]    => fromReflect(d.value)
      case s: Reflect.Sequence[F, a, b] => fromSequence[F, a, b](s)
      case m: Reflect.Map[F, a, b, m]   => fromMap[F, a, b, m](m)
      case d: Reflect.Dynamic[F]        => fromDynamic(d)
    }

  private def fromPrimitive[F[_, _], A](
    prim: Reflect.Primitive[F, A]
  )(implicit config: JsonSchemaConfig): JsonSchema[A] =
    if (config.includeZioExtensions) {
      `zio-primitive`[A](
        primitiveType = prim.primitiveType.typeName.name,
        title = Some(prim.typeName.name),
        description = if (prim.doc != Doc.Empty) Some(docToString(prim.doc)) else None,
        zioValidations =
          if (config.includeValidations) extractValidations(prim.primitiveType).getOrElse(Seq.empty) else Seq.empty,
        zioModifiers = if (config.includeModifiers) extractModifiers(prim.modifiers) else Seq.empty
      )
    } else {
      // Create standard JsonSchema types
      jsonType(prim.primitiveType) match {
        case "boolean" => `boolean`.asInstanceOf[JsonSchema[A]]
        case "integer" => `integer`.asInstanceOf[JsonSchema[A]]
        case "number"  => `number`.asInstanceOf[JsonSchema[A]]
        case "string"  =>
          val format = extractFormat(prim.primitiveType)
          `string`(format).asInstanceOf[JsonSchema[A]]
        case _ => `string`().asInstanceOf[JsonSchema[A]]
      }
    }

  private def fromRecord[F[_, _], A](
    rec: Reflect.Record[F, A]
  )(implicit config: JsonSchemaConfig): JsonSchema[A] = {
    val properties = ListMap.from(rec.fields.map { f =>
      f.name -> fromReflect(f.value)
    })

    val required = rec.fields.map(_.name).toSeq

    if (config.includeZioExtensions) {
      `zio-record`[A](
        typeName = rec.typeName,
        fields = properties,
        required = required,
        title = Some(rec.typeName.name),
        description = if (rec.doc != Doc.Empty) Some(docToString(rec.doc)) else None,
        zioModifiers = if (config.includeModifiers) extractModifiers(rec.modifiers) else Seq.empty
      )
    } else {
      val open = openRecordFlag(rec.typeName)
      `object`[A](properties, required, additionalProperties = open)
    }
  }

  private def fromVariant[F[_, _], A](
    variant: Reflect.Variant[F, A]
  )(implicit config: JsonSchemaConfig): JsonSchema[A] = {
    val cases = ListMap.from(variant.cases.map { c =>
      c.name -> fromReflect(c.value)
    })

    if (config.includeZioExtensions) {
      `zio-variant`[A](
        typeName = variant.typeName,
        cases = cases,
        title = Some(variant.typeName.name),
        description = if (variant.doc != Doc.Empty) Some(docToString(variant.doc)) else None,
        zioModifiers = if (config.includeModifiers) extractModifiers(variant.modifiers) else Seq.empty
      )
    } else {
      val alternatives = variant.cases.map { c =>
        `object`[A](
          properties = ListMap(
            "tag"   -> `string`(),
            "value" -> fromReflect(c.value)
          ),
          required = ArraySeq("tag", "value"),
          additionalProperties = openRecordFlagName(c.name)
        )
      }
      `oneof`[A](alternatives)
    }
  }

  private def fromSequence[F[_, _], A, C[_]](
    seq: Reflect.Sequence[F, A, C]
  )(implicit config: JsonSchemaConfig): JsonSchema[C[A]] = {
    val elementSchema = fromReflect(seq.element)
    `array`(elementSchema).asInstanceOf[JsonSchema[C[A]]]
  }

  private def fromMap[F[_, _], K, V, M[_, _]](
    map: Reflect.Map[F, K, V, M]
  ): JsonSchema[M[K, V]] =
    `object`[M[K, V]](
      properties = ListMap.empty, // Maps use additionalProperties instead
      required = ArraySeq.empty,
      additionalProperties = Some(true),
      title = Some(map.typeName.name),
      description = if (map.doc != Doc.Empty) Some(docToString(map.doc)) else None
    )

  private def fromDynamic[F[_, _]](
    dyn: Reflect.Dynamic[F]
  ): JsonSchema[DynamicValue] =
    `object`[DynamicValue](
      properties = ListMap.empty,
      required = ArraySeq.empty,
      additionalProperties = Some(true),
      title = Some(dyn.typeName.name),
      description = if (dyn.doc != Doc.Empty) Some(docToString(dyn.doc)) else None
    )

  private def fromWrapper[F[_, _], A, B](
    wrap: Reflect.Wrapper[F, A, B]
  )(implicit config: JsonSchemaConfig): JsonSchema[A] =
    fromReflect(wrap.wrapped).asInstanceOf[JsonSchema[A]]

  // Helper methods
  private def jsonType(pt: PrimitiveType[?]): String = pt match {
    case PrimitiveType.Unit | _: PrimitiveType.Unit.type => "null"
    case _: PrimitiveType.Boolean                        => "boolean"
    case _: PrimitiveType.Byte | _: PrimitiveType.Short | _: PrimitiveType.Int | _: PrimitiveType.Long |
        _: PrimitiveType.BigInt =>
      "integer"
    case _: PrimitiveType.Float | _: PrimitiveType.Double | _: PrimitiveType.BigDecimal => "number"
    case _                                                                              => "string"
  }

  private def primitiveTypeToJsonType(primitiveType: String): String = primitiveType match {
    case "Unit"                                       => "null"
    case "Boolean"                                    => "boolean"
    case "Byte" | "Short" | "Int" | "Long" | "BigInt" => "integer"
    case "Float" | "Double" | "BigDecimal"            => "number"
    case _                                            => "string"
  }

  private def docToString(doc: Doc): String = {
    val sb = new StringBuilder
    doc.flatten.foreach { case Doc.Text(v) => sb.append(v); case _ => () }
    sb.toString
  }

  private def openRecordFlag(tn: TypeName[?])(implicit config: JsonSchemaConfig): Option[Boolean] =
    openRecordFlagName(typeNameToString(tn))

  private def openRecordFlagName(name: String)(implicit config: JsonSchemaConfig): Option[Boolean] =
    config.additionalProperties.collectFirst { case (rx, flag) if rx.findFirstIn(name).isDefined => flag }.flatten

  private def extractFormat(pt: PrimitiveType[?]): Option[String] = pt match {
    case _: PrimitiveType.Instant        => Some("date-time")
    case _: PrimitiveType.LocalDate      => Some("date")
    case _: PrimitiveType.LocalTime      => Some("time")
    case _: PrimitiveType.LocalDateTime  => Some("date-time")
    case _: PrimitiveType.OffsetDateTime => Some("date-time")
    case _: PrimitiveType.OffsetTime     => Some("time")
    case _: PrimitiveType.ZonedDateTime  => Some("date-time")
    case _: PrimitiveType.Duration       => Some("duration")
    case _: PrimitiveType.Period         => Some("duration")
    case _: PrimitiveType.UUID           => Some("uuid")
    case _: PrimitiveType.Currency       => Some("currency")
    case _                               => None
  }

  private def extractValidations(pt: PrimitiveType[?]): Option[Seq[ZioValidation]] = {
    val validations = pt.validation match {
      case Validation.None             => Seq.empty
      case Validation.Numeric.Positive =>
        Seq(ZioValidation("numeric", "positive", Some("Must be positive")))
      case Validation.Numeric.Negative =>
        Seq(ZioValidation("numeric", "negative", Some("Must be negative")))
      case Validation.Numeric.NonPositive =>
        Seq(ZioValidation("numeric", "nonPositive", Some("Must be non-positive")))
      case Validation.Numeric.NonNegative =>
        Seq(ZioValidation("numeric", "nonNegative", Some("Must be non-negative")))
      case Validation.Numeric.Range(min, max) =>
        Seq(
          min.map(m => ZioValidation("numeric", s"minimum:$m", Some(s"Must be >= $m"))),
          max.map(m => ZioValidation("numeric", s"maximum:$m", Some(s"Must be <= $m")))
        ).flatten
      case Validation.Numeric.Set(values) =>
        Seq(
          ZioValidation(
            "numeric",
            s"enum:${values.values.toString}",
            Some(s"Must be one of: ${values.values.toString}")
          )
        )
      case Validation.String.NonEmpty =>
        Seq(ZioValidation("string", "nonEmpty", Some("Must not be empty")))
      case Validation.String.Empty =>
        Seq(ZioValidation("string", "empty", Some("Must be empty")))
      case Validation.String.Blank =>
        Seq(ZioValidation("string", "blank", Some("Must be blank")))
      case Validation.String.NonBlank =>
        Seq(ZioValidation("string", "nonBlank", Some("Must not be blank")))
      case Validation.String.Length(min, max) =>
        val constraints = Seq(
          min.map(m => ZioValidation("string", s"minLength:$m", Some(s"Must have at least $m characters"))),
          max.map(m => ZioValidation("string", s"maxLength:$m", Some(s"Must have at most $m characters")))
        ).flatten
        constraints
      case p: Validation.String.Pattern =>
        Seq(ZioValidation("string", s"pattern:${p.regex}", Some(s"Must match pattern: ${p.regex}")))
    }

    if (validations.nonEmpty) Some(validations) else None
  }

  private def extractModifiers(modifiers: Seq[Modifier]): Seq[ZioModifier] =
    modifiers.flatMap {
      case Modifier.config(k, v) => ZioModifier("config", Some(k), Some(v), None) :: Nil
      case _: Modifier.transient => Nil
    }

  /**
   * Convert a Reflect to a proper JSON Schema DynamicValue. The result will be
   * a valid JSON Schema that can be serialized to JSON.
   */
  def toJsonSchema[F[_, _], A](
    r: Reflect[F, A]
  )(implicit config: JsonSchemaConfig): DynamicValue = {
    val ctx = new GenCtx(
      mutable.LinkedHashMap.empty[String, DynamicValue],
      mutable.HashSet.empty[String],
      mutable.HashMap.empty[String, DynamicValue]
    )
    val root = reflectToJsonSchemaDynamicWithCtx(r, config, ctx)
    if (ctx.defs.isEmpty) root
    else
      root match {
        case DynamicValue.Record(fields) =>
          val defsRecord = DynamicValue.Record(ArraySeq.from(ctx.defs.toSeq))
          // If root is a pure $ref, inline referenced body and include $defs
          fields.collectFirst { case ("$ref", DynamicValue.Primitive(PrimitiveValue.String(path))) => path } match {
            case Some(path) =>
              val key = {
                val idx = path.lastIndexOf('/')
                if (idx >= 0 && idx + 1 < path.length) path.substring(idx + 1) else path
              }
              ctx.defs.get(key) match {
                case Some(DynamicValue.Record(bodyFields)) =>
                  DynamicValue.Record(bodyFields :+ ("$defs" -> defsRecord))
                case Some(body) =>
                  DynamicValue.Record(
                    ArraySeq(
                      "$ref"  -> body,
                      "$defs" -> defsRecord
                    )
                  )
                case None =>
                  DynamicValue.Record(fields :+ ("$defs" -> defsRecord))
              }
            case None =>
              DynamicValue.Record(fields :+ ("$defs" -> defsRecord))
          }
        case other =>
          val defsRecord = DynamicValue.Record(ArraySeq.from(ctx.defs.toSeq))
          other match {
            case DynamicValue.Record(refFields) =>
              // If root is a $ref, inline the referenced body and include $defs
              refFields.collectFirst { case ("$ref", DynamicValue.Primitive(PrimitiveValue.String(path))) =>
                path
              } match {
                case Some(path) =>
                  val key = {
                    val idx = path.lastIndexOf('/')
                    if (idx >= 0 && idx + 1 < path.length) path.substring(idx + 1) else path
                  }
                  ctx.defs.get(key) match {
                    case Some(DynamicValue.Record(bodyFields)) =>
                      DynamicValue.Record(bodyFields :+ ("$defs" -> defsRecord))
                    case Some(body) =>
                      DynamicValue.Record(
                        ArraySeq(
                          "$ref"  -> body,
                          "$defs" -> defsRecord
                        )
                      )
                    case None =>
                      DynamicValue.Record(
                        ArraySeq(
                          "$ref"  -> other,
                          "$defs" -> defsRecord
                        )
                      )
                  }
                case None =>
                  DynamicValue.Record(
                    ArraySeq(
                      "$ref"  -> other,
                      "$defs" -> defsRecord
                    )
                  )
              }
            case _ =>
              DynamicValue.Record(
                ArraySeq(
                  "$ref"  -> other,
                  "$defs" -> defsRecord
                )
              )
          }
      }
  }

  /**
   * Convert a JSON Schema DynamicValue back to an unbound Reflect. This allows
   * deserializing schemas from JSON Schema format.
   */
  def fromJsonSchema(value: DynamicValue): Either[SchemaError, Reflect[NoBinding, ?]] =
    dynamicValueToReflect(value)

  // Context for $defs/$ref emission and cycle detection
  private final case class GenCtx(
    defs: mutable.LinkedHashMap[String, DynamicValue],
    visiting: mutable.HashSet[String],
    memo: mutable.HashMap[String, DynamicValue]
  )

  private def defKey(tn: TypeName[?]): String = typeNameToString(tn)

  private def makeRef(key: String): DynamicValue =
    dvRecord(ArraySeq("$ref" -> dvString(s"#/$$defs/$key")))

  private def reflectToJsonSchemaDynamicWithCtx[F[_, _], A](
    reflect: Reflect[F, A],
    config: JsonSchemaConfig,
    ctx: GenCtx
  ): DynamicValue = {
    reflect match {

      case reflect @ zio.blocks.schema.Reflect.Map(_, _, _, _, _, _) =>
        val map = reflect.asMap.get
        val key = defKey(map.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          fields += "additionalProperties"                  -> reflectToJsonSchemaDynamicWithCtx(map.value, config, ctx)
          if (map.doc != Doc.Empty) fields += "description" -> dvString(docToString(map.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("map")
            fields += config.typeNameKey -> dvString(typeNameToString(map.typeName))
            fields += config.keyKey      -> reflectToJsonSchemaDynamicWithCtx(map.key, config, ctx)
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case reflect @ zio.blocks.schema.Reflect.Dynamic(_, _, _, _) => {
        val dyn = reflect.asDynamic.get
        val key = defKey(dyn.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
        }
        val fields = ArraySeq.newBuilder[(String, DynamicValue)]
        fields += "type"                                  -> dvString("object")
        if (dyn.doc != Doc.Empty) fields += "description" -> dvString(docToString(dyn.doc))
        if (config.includeZioExtensions) {
          fields += config.nodeTypeKey -> dvString("dynamic")
          fields += config.typeNameKey -> dvString(typeNameToString(dyn.typeName))
        }
        val body = dvRecord(fields.result())
        ctx.visiting -= key
        ctx.defs.put(key, body)
        val ref = makeRef(key)
        ctx.memo.put(key, ref)
        ref
      }

      case p: Reflect.Primitive[F, A] =>
        val prim = p
        val key  = defKey(prim.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type" -> dvString(jsonType(prim.primitiveType))
          extractFormat(prim.primitiveType).foreach(f => fields += "format" -> dvString(f))
          extractMinimum(prim.primitiveType).foreach(m => fields += "minimum" -> dvNumber(m))
          extractMaximum(prim.primitiveType).foreach(m => fields += "maximum" -> dvNumber(m))
          extractMinLength(prim.primitiveType).foreach(l => fields += "minLength" -> dvInt(l))
          extractMaxLength(prim.primitiveType).foreach(l => fields += "maxLength" -> dvInt(l))
          extractPattern(prim.primitiveType).foreach(px => fields += "pattern" -> dvString(px))
          if (prim.doc != Doc.Empty) fields += "description" -> dvString(docToString(prim.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey  -> dvString("primitive")
            fields += config.primitiveKey -> dvString(prim.primitiveType.typeName.name)
            fields += config.typeNameKey  -> dvString(typeNameToString(prim.typeName))
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case rec: Reflect.Record[F, A] =>
        val key = defKey(rec.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val props = rec.fields.map { f =>
            f.name -> reflectToJsonSchemaDynamicWithCtx(f.value, config, ctx)
          }
          val required = rec.fields.map(_.name)
          val fields   = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"       -> dvString("object")
          fields += "properties" -> dvRecord(ArraySeq.from(props))
          fields += "required"   -> dvSequence(ArraySeq.from(required.map(dvString)))
          openRecordFlag(rec.typeName).foreach(b => fields += "additionalProperties" -> dvBoolean(b))
          if (rec.doc != Doc.Empty) fields += "description" -> dvString(docToString(rec.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("record")
            fields += config.typeNameKey -> dvString(typeNameToString(rec.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case variant: Reflect.Variant[F, A] =>
        val key = defKey(variant.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val oneOf = variant.cases.map { c =>
            DynamicValue.Record(
              ArraySeq(
                "type"       -> dvString("object"),
                "properties" -> Record(
                  ArraySeq(
                    "tag"   -> Record(ArraySeq("const" -> dvString(c.name))),
                    "value" -> reflectToJsonSchemaDynamicWithCtx(c.value, config, ctx)
                  )
                ),
                "additionalProperties" -> openRecordFlag(variant.typeName).map(dvBoolean).getOrElse(dvBoolean(false)),
                "required"             -> DynamicValue.Sequence(ArraySeq(dvString("tag"), dvString("value")))
              )
            )
          }
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "oneOf"                                     -> DynamicValue.Sequence(ArraySeq.from(oneOf))
          if (variant.doc != Doc.Empty) fields += "description" -> dvString(docToString(variant.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("variant")
            fields += config.typeNameKey -> dvString(typeNameToString(variant.typeName))
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case _ if reflect.isMap =>
        val map     = reflect.asInstanceOf[Reflect.Map[F, Any, Any, Map]].asMap.get
        val keyName = defKey(map.typeName)
        if (ctx.memo.contains(keyName)) ctx.memo(keyName)
        else if (ctx.defs.contains(keyName) || ctx.visiting.contains(keyName)) makeRef(keyName)
        else {
          ctx.visiting += keyName
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          fields += "additionalProperties"                  -> reflectToJsonSchemaDynamicWithCtx(map.value, config, ctx)
          if (map.doc != Doc.Empty) fields += "description" -> dvString(docToString(map.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("map")
            fields += config.typeNameKey -> dvString(typeNameToString(map.typeName))
            fields += config.keyKey      -> reflectToJsonSchemaDynamicWithCtx(map.key, config, ctx)
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= keyName
          ctx.defs.put(keyName, body)
          val ref = makeRef(keyName)
          ctx.memo.put(keyName, ref)
          ref
        }

      case d if reflect.isDynamic =>
        val dyn = d.asDynamic.get
        val key = defKey(dyn.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          if (dyn.doc != Doc.Empty) fields += "description" -> dvString(docToString(dyn.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("dynamic")
            fields += config.typeNameKey -> dvString(typeNameToString(dyn.typeName))
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case wrap: Reflect.Wrapper[F, A, Any] =>
        val key = defKey(wrap.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val wrappedSchema = reflectToJsonSchemaDynamicWithCtx(wrap.wrapped, config, ctx)
          val body          = if (config.includeZioExtensions) {
            val fields = ArraySeq.newBuilder[(String, DynamicValue)]
            wrappedSchema match {
              case DynamicValue.Record(wrappedFields) => fields ++= wrappedFields
              case _                                  => fields += "type" -> dvString("object")
            }
            fields += config.nodeTypeKey                       -> dvString("wrapper")
            fields += config.typeNameKey                       -> dvString(typeNameToString(wrap.typeName))
            fields += config.wrappedKey                        -> wrappedSchema
            if (wrap.doc != Doc.Empty) fields += "description" -> dvString(docToString(wrap.doc))
            DynamicValue.Record(fields.result())
          } else wrappedSchema
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case seq: Reflect.Sequence[F, ?, ?] =>
        val key = defKey(seq.typeName)
        if (ctx.memo.contains(key)) ctx.memo(key)
        else if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("array")
          fields += "items"                                 -> reflectToJsonSchemaDynamicWithCtx(seq.element, config, ctx)
          if (seq.doc != Doc.Empty) fields += "description" -> dvString(docToString(seq.doc))
          if (config.includeZioExtensions) {
            fields += config.nodeTypeKey -> dvString("sequence")
            fields += config.typeNameKey -> dvString(typeNameToString(seq.typeName))
          }
          val body = dvRecord(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          val ref = makeRef(key)
          ctx.memo.put(key, ref)
          ref
        }

      case d: Reflect.Deferred[F, A] =>
        reflectToJsonSchemaDynamicWithCtx(d.value, config, ctx)
    }
  }

  private def dynamicValueToReflect(
    value: DynamicValue,
    defs: Map[String, DynamicValue] = Map.empty
  )(implicit config: JsonSchemaConfig): Either[SchemaError, Reflect[NoBinding, ?]] = {

    def asString(dv: DynamicValue): Option[String] = dv match {
      case DynamicValue.Primitive(PrimitiveValue.String(s)) => Some(s)
      case _                                                => None
    }

    def fieldsOf(rec: DynamicValue): Option[ArraySeq[(String, DynamicValue)]] =
      rec match {
        case DynamicValue.Record(f) => Some(ArraySeq.from(f))
        case _                      => None
      }

    def get(rec: ArraySeq[(String, DynamicValue)], key: String): Option[DynamicValue] = {
      var i = rec.length - 1
      while (i >= 0) {
        val kv = rec(i)
        if (kv._1 == key) return Some(kv._2)
        i -= 1
      }
      None
    }

    def stripDefs(
      rec: ArraySeq[(String, DynamicValue)]
    ): (ArraySeq[(String, DynamicValue)], Map[String, DynamicValue]) = {
      val defs     = get(rec, "$defs").flatMap(fieldsOf).map(_.toSeq.toMap).getOrElse(ListMap.empty)
      val filtered = rec.filter(_._1 != "$defs")
      (filtered, defs)
    }

    def resolveRef(path: String, defs: Map[String, DynamicValue]): Option[DynamicValue] = {
      val idx = path.lastIndexOf('/')
      if (idx >= 0 && idx + 1 < path.length) defs.get(path.substring(idx + 1)) else None
    }

    def parseTypeName(s: String): TypeName[Any] = {
      val lastDot = s.lastIndexOf('.')
      if (lastDot < 0) TypeName[Any](Namespace(Nil), s)
      else TypeName[Any](Namespace(s.substring(0, lastDot).split('.').toList), s.substring(lastDot + 1))
    }

    def parsePrimitive(tpe: String, format: Option[String]): Reflect[NoBinding, ?] =
      (tpe, format) match {
        case ("boolean", _)                => Reflect.boolean[Binding].noBinding
        case ("integer", _)                => Reflect.int[Binding].noBinding
        case ("number", _)                 => Reflect.double[Binding].noBinding
        case ("string", Some("uuid"))      => Reflect.uuid[Binding].noBinding
        case ("string", Some("date"))      => Reflect.localDate[Binding].noBinding
        case ("string", Some("time"))      => Reflect.localTime[Binding].noBinding
        case ("string", Some("date-time")) => Reflect.localDateTime[Binding].noBinding
        case ("string", _)                 => Reflect.string[Binding].noBinding
        case ("null", _)                   => Reflect.unit[Binding].noBinding
        case _                             => Reflect.string[Binding].noBinding
      }

    def parseObject[A](
      rec: ArraySeq[(String, DynamicValue)],
      defs: Map[String, DynamicValue]
    )(implicit config: JsonSchemaConfig): Either[SchemaError, Reflect[NoBinding, A]] = {
      val (props, defs2) = stripDefs(rec)
      val title          = get(rec, "title").flatMap(asString)
      val description    = get(rec, "description").flatMap(asString)
      val tn             = get(rec, config.typeNameKey)
        .flatMap(asString)
        .map(parseTypeName)
        .orElse(title.map(parseTypeName))
        .getOrElse(TypeName[A](Namespace(Nil), "Object"))
      val doc = description.map(Doc.Text(_)).getOrElse(Doc.Empty)

      val defsOut = defs ++ defs2

      val required = get(rec, "required").flatMap(asString)

      val terms = props.map { case (name, schemaDv) =>
        // use required to fail if available
        if (required.contains(name)) {
          dynamicValueToReflect(schemaDv, defsOut) match {
            case Right(fv) => Right(Term(name, fv, doc, Nil))
            case Left(e)   => Left(e)
          }
        } else {
          dynamicValueToReflect(schemaDv, defsOut) match {
            case Right(fv) => Right(Term(name, fv, doc, Nil))
            case Left(e)   => Left(e)
          }
        }
      }

      val firstErr = terms.collectFirst { case Left(e) => e }
      firstErr match {
        case Some(e) => Left(e)
        case None    =>
          val fields = terms.collect { case Right(t) => t }.toIndexedSeq
          Right(
            Reflect.Record(
              fields.asInstanceOf[IndexedSeq[Term[NoBinding, A, ?]]],
              tn.asInstanceOf[TypeName[A]],
              binding.NoBinding(),
              doc,
              Nil
            )
          )
      }
    }

    def parseArray(
      rec: ArraySeq[(String, DynamicValue)],
      defs: Map[String, DynamicValue]
    )(implicit config: JsonSchemaConfig): Either[SchemaError, Reflect[NoBinding, Any]] =
      get(rec, "items") match {
        case Some(itemsDv) =>
          dynamicValueToReflect(itemsDv, defs) match {
            case Right(elem) =>
              val tn = get(rec, config.typeNameKey)
                .flatMap(asString)
                .map(parseTypeName)
                .getOrElse(TypeName[Any](Namespace(Nil), "Unit"))
              Right(
                Reflect
                  .Sequence[NoBinding, Any, List](
                    elem.asInstanceOf[Reflect[NoBinding, Any]],
                    tn.asInstanceOf[TypeName[List[Any]]],
                    binding.NoBinding(),
                    Doc.Empty,
                    Nil
                  )
                  .asInstanceOf[Reflect[NoBinding, Any]]
              )
            case Left(e) => Left(e)
          }
        case None => Left(SchemaError.invalidType(Nil, "array items missing"))
      }

    def parseOneOf(
      rec: ArraySeq[(String, DynamicValue)],
      defs: Map[String, DynamicValue]
    )(implicit config: JsonSchemaConfig): Either[SchemaError, Reflect[NoBinding, Any]] =
      get(rec, "oneOf") match {
        case Some(DynamicValue.Sequence(alts)) =>
          val parsed =
            alts.map {
              case DynamicValue.Record(f) =>
                val tag = for {
                  propsDv <- get(ArraySeq.from(f), "properties")
                  props   <- fieldsOf(propsDv)
                  tagRec  <- get(props, "tag").flatMap(fieldsOf)
                  const    = get(tagRec, "const").flatMap(asString)
                } yield const.getOrElse("case")
                val valueSchema = for {
                  propsDv <- get(ArraySeq.from(f), "properties")
                  props   <- fieldsOf(propsDv)
                  v       <- get(props, "value")
                } yield v
                (tag, valueSchema) match {
                  case (Some(name), Some(vdv)) =>
                    dynamicValueToReflect(vdv, defs) match {
                      case Right(rv) => Right(Term(name, rv, Doc.Empty, Nil))
                      case Left(e)   => Left(e)
                    }
                  case _ => Left(SchemaError.invalidType(Nil, "invalid oneOf alternative"))
                }
              case _ => Left(SchemaError.invalidType(Nil, "invalid oneOf alternative"))
            }

          val firstErr = parsed.collectFirst { case Left(e) => e }
          firstErr match {
            case Some(e) => Left(e)
            case None    =>

              val tn = get(rec, config.typeNameKey)
                .flatMap(asString)
                .map(parseTypeName)
                .getOrElse(TypeName[Any](Namespace(Nil), "Variant"))

              val cases = parsed.collect { case Right(t) => t }.toIndexedSeq
                .asInstanceOf[IndexedSeq[Term[NoBinding, Any, ? <: Any]]]

              Right(
                Reflect
                  .Variant[NoBinding, Any](cases, tn.asInstanceOf[TypeName[Any]], binding.NoBinding(), Doc.Empty, Nil)
              )
          }

        case _ => Left(SchemaError.invalidType(Nil, "oneOf missing"))
      }

    def parseVariant(
      rec: ArraySeq[(String, DynamicValue)],
      defs: Map[String, DynamicValue]
    )(implicit config: JsonSchemaConfig): Either[SchemaError, Reflect[NoBinding, ?]] =
      get(rec, "variant") match {
        case Some(DynamicValue.Record(all)) =>
          val (root, defs2) = stripDefs(ArraySeq.from(all))
          val defsOut       = defs2 ++ defs
          get(root, "$ref").flatMap(asString).flatMap(path => resolveRef(path, defsOut)) match {
            case Some(body) => dynamicValueToReflect(body, defsOut)
            case None       =>
              val tpe = get(root, "type").flatMap(asString)
              tpe match {
                case Some("object")  => parseObject(root, defsOut)
                case Some("array")   => parseArray(root, defsOut)
                case Some("oneOf")   => parseOneOf(root, defsOut)
                case Some("variant") => parseVariant(root, defsOut)
                case _               =>
                  val fmt = get(root, "format").flatMap(asString)
                  Right(parsePrimitive(tpe.getOrElse("string"), fmt))
              }
          }
        case _ => Left(SchemaError.invalidType(Nil, "Expected object schema"))
      }

    value match {
      case DynamicValue.Record(all) =>
        val (root, defs2) = stripDefs(ArraySeq.from(all))
        val defsOut       = defs2 ++ defs
        get(root, "$ref").flatMap(asString(_)).flatMap(path => resolveRef(path, defsOut)) match {
          case Some(body) => dynamicValueToReflect(body, defsOut)
          case None       =>
            val tpe      = get(root, "type").flatMap(asString)
            val nodeType = get(root, config.nodeTypeKey).flatMap(asString)
            // Wrapper and Dynamic support via nodeTypeKey
            nodeType match {
              case Some("wrapper") =>
                get(root, config.wrappedKey) match {
                  case Some(wrapped) =>
                    dynamicValueToReflect(wrapped, defsOut) match {
                      case Right(inner) =>
                        val tn = get(root, config.typeNameKey)
                          .flatMap(asString)
                          .map(parseTypeName)
                          .getOrElse(TypeName[Any](Namespace(Nil), "Wrapper"))
                          .asInstanceOf[TypeName[Any]]
                        Right(
                          Reflect
                            .Wrapper[NoBinding, Any, Any](
                              inner.asInstanceOf[Reflect[NoBinding, Any]],
                              tn.asInstanceOf[TypeName[Any]],
                              binding.NoBinding(),
                              Doc.Empty,
                              Nil
                            )
                            .asInstanceOf[Reflect[NoBinding, Any]]
                        )
                      case Left(e) => Left(e)
                    }
                  case None => Left(SchemaError.invalidType(Nil, "wrapper missing wrapped"))
                }
              case Some("dynamic") => Right(Reflect.Dynamic(binding.NoBinding()))
              case Some("record")  => parseObject(root, defsOut)
              case Some("variant") => parseOneOf(root, defsOut)
              case _               =>
                tpe match {
                  case Some("object") =>
                    // Detect map via additionalProperties when properties are absent
                    (get(root, "properties"), get(root, "additionalProperties")) match {
                      case (None | Some(DynamicValue.Record(ArraySeq())), Some(ap)) =>
                        dynamicValueToReflect(ap, defsOut) match {
                          case Right(valueRefl) =>
                            val keyRefl = Reflect.string[Binding].noBinding.asInstanceOf[Reflect[NoBinding, Any]]
                            val tn      = get(root, config.typeNameKey)
                              .flatMap(asString)
                              .map(parseTypeName)
                              .getOrElse(TypeName[Any](Namespace(Nil), "Unit"))
                            Right(
                              Reflect
                                .Map[NoBinding, Any, Any, collection.immutable.Map](
                                  keyRefl,
                                  valueRefl.asInstanceOf[Reflect[NoBinding, Any]],
                                  tn.asInstanceOf[TypeName[collection.immutable.Map[Any, Any]]],
                                  binding.NoBinding(),
                                  Doc.Empty,
                                  Nil
                                )
                                .asInstanceOf[Reflect[NoBinding, Any]]
                            )
                          case Left(e) => Left(e)
                        }
                      case _ => parseObject(root, defsOut)
                    }
                  case Some("array")                        => parseArray(root, defsOut)
                  case Some("oneOf")                        => parseOneOf(root, defsOut)
                  case Some("variant")                      => parseVariant(root, defsOut)
                  case None if get(root, "oneOf").isDefined => parseOneOf(root, defsOut)
                  case _                                    =>
                    val fmt = get(root, "format").flatMap(asString)
                    Right(parsePrimitive(tpe.getOrElse("string"), fmt))
                }
            }
        }
      case _ => Left(SchemaError.invalidType(Nil, "Expected object schema"))
    }
  }

  // Helper methods for DynamicValue creation
  private def dvString(s: String): DynamicValue                                = DynamicValue.Primitive(prim.String(s))
  private def dvInt(i: Int): DynamicValue                                      = DynamicValue.Primitive(PrimitiveValue.Int(i))
  private def dvNumber(n: BigDecimal): DynamicValue                            = DynamicValue.Primitive(prim.BigDecimal(n))
  private def dvBoolean(b: Boolean): DynamicValue                              = DynamicValue.Primitive(prim.Boolean(b))
  private def dvRecord(fields: ArraySeq[(String, DynamicValue)]): DynamicValue = DynamicValue.Record(fields)
  private def dvRecord(fields: (String, DynamicValue)*): DynamicValue          = DynamicValue.Record(fields.toIndexedSeq)
  private def dvSequence(items: ArraySeq[DynamicValue]): DynamicValue          = DynamicValue.Sequence(items)
  private def typeNameToString(tn: TypeName[?]): String                        = {
    val ns = tn.namespace.elements.mkString(".")
    if (ns.isEmpty) tn.name else s"$ns.${tn.name}"
  }

  private def extractMinimum(pt: PrimitiveType[?]): Option[BigDecimal] =
    pt.validation match {
      case Validation.Numeric.Range(Some(min), _) =>
        min match {
          case i: Int         => Some(BigDecimal(i))
          case l: Long        => Some(BigDecimal(l))
          case f: Float       => Some(BigDecimal(f.toDouble))
          case d: Double      => Some(BigDecimal(d))
          case bd: BigDecimal => Some(bd)
          case bi: BigInt     => Some(BigDecimal(bi))
          case _              => None
        }
      case Validation.Numeric.Positive    => Some(BigDecimal(0))
      case Validation.Numeric.NonNegative => Some(BigDecimal(0))
      case _                              => None
    }

  private def extractMaximum(pt: PrimitiveType[?]): Option[BigDecimal] =
    pt.validation match {
      case Validation.Numeric.Range(_, Some(max)) =>
        max match {
          case i: Int         => Some(BigDecimal(i))
          case l: Long        => Some(BigDecimal(l))
          case f: Float       => Some(BigDecimal(f.toDouble))
          case d: Double      => Some(BigDecimal(d))
          case bd: BigDecimal => Some(bd)
          case bi: BigInt     => Some(BigDecimal(bi))
          case _              => None
        }
      case Validation.Numeric.Negative    => Some(BigDecimal(0))
      case Validation.Numeric.NonPositive => Some(BigDecimal(0))
      case _                              => None
    }

  private def extractMinLength(pt: PrimitiveType[?]): Option[Int] =
    pt.validation match {
      case Validation.String.Length(Some(min), _) => Some(min)
      case Validation.String.NonEmpty             => Some(1)
      case Validation.String.NonBlank             => Some(1)
      case _                                      => None
    }

  private def extractMaxLength(pt: PrimitiveType[?]): Option[Int] =
    pt.validation match {
      case Validation.String.Length(_, Some(max)) => Some(max)
      case Validation.String.Empty                => Some(0)
      case Validation.String.Blank                => Some(0)
      case _                                      => None
    }

  private def extractPattern(pt: PrimitiveType[?]): Option[String] =
    pt.validation match {
      case p: Validation.String.Pattern => Some(p.regex)
      case _                            => None
    }

  private def schemaToDynamicValue(
    schema: JsonSchema[?]
  )(implicit config: JsonSchemaConfig): DynamicValue =
    // Convert our JsonSchema AST to DynamicValue for backward compatibility
    schema match {
      case `boolean` =>
        DynamicValue.Record(
          ArraySeq(
            "type" -> DynamicValue.Primitive(PrimitiveValue.String("boolean"))
          )
        )
      case `integer` =>
        DynamicValue.Record(
          ArraySeq(
            "type" -> DynamicValue.Primitive(PrimitiveValue.String("integer"))
          )
        )
      case `number` =>
        DynamicValue.Record(
          ArraySeq(
            "type" -> DynamicValue.Primitive(PrimitiveValue.String("number"))
          )
        )
      case s: `string` =>
        val fields = ArraySeq.newBuilder[(String, DynamicValue)]
        fields += "type" -> DynamicValue.Primitive(PrimitiveValue.String("string"))
        s.format.foreach(f => fields += "format" -> DynamicValue.Primitive(PrimitiveValue.String(f)))
        DynamicValue.Record(fields.result())
      case a: `array`[?] =>
        DynamicValue.Record(
          ArraySeq(
            "type"  -> DynamicValue.Primitive(PrimitiveValue.String("array")),
            "items" -> schemaToDynamicValue(a.items)
          )
        )
      case o: `object`[?] =>
        val props = o.properties.map { case (name, schema) =>
          name -> schemaToDynamicValue(schema)
        }
        dvRecord(
          "type"                 -> DynamicValue.Primitive(PrimitiveValue.String("object")),
          "properties"           -> DynamicValue.Record(ArraySeq.from(props)),
          "additionalProperties" -> o.additionalProperties.map(dvBoolean).getOrElse(dvBoolean(false)),
          "required"             -> dvSequence(
            ArraySeq.from(o.required.map(s => dvString(s)))
          )
        )
      case p: `zio-primitive`[?] =>
        val fields = ArraySeq.newBuilder[(String, DynamicValue)]
        fields += "type"              -> dvString(p.jsonType)
        fields += config.nodeTypeKey  -> DynamicValue.Primitive(PrimitiveValue.String("primitive"))
        fields += config.primitiveKey -> DynamicValue.Primitive(PrimitiveValue.String(p.primitiveType))
        p.title.foreach(t => fields += "title" -> DynamicValue.Primitive(PrimitiveValue.String(t)))
        p.description.foreach(d => fields += "description" -> DynamicValue.Primitive(PrimitiveValue.String(d)))
        DynamicValue.Record(fields.result())

      case r: `zio-record`[?] =>
        val props  = r.fields.map { case (name, schema) => name -> schemaToDynamicValue(schema) }
        val fields = ArraySeq.newBuilder[(String, DynamicValue)]
        fields += "type"             -> dvString("object")
        fields += config.nodeTypeKey -> dvString("record")
        fields += config.typeNameKey -> dvString(typeNameToString(r.typeName))
        fields += "properties"       -> dvRecord(ArraySeq.from(props))
        fields += "required"         -> dvSequence(
          ArraySeq.from(
            r.required.map(s => DynamicValue.Primitive(PrimitiveValue.String(s)))
          )
        )
        r.description.foreach(d => fields += "description" -> dvString(d))
        r.title.foreach(t => fields += "title" -> dvString(t))
        openRecordFlag(r.typeName).foreach(b => fields += "additionalProperties" -> dvBoolean(b))
        DynamicValue.Record(fields.result())

      case v: `zio-variant`[?] =>
        val oneOf = v.cases.map { case (name, schema) =>
          DynamicValue.Record(
            ArraySeq(
              "type"       -> dvString("object"),
              "properties" -> dvRecord(
                ArraySeq(
                  "tag" -> dvRecord(
                    ArraySeq("const" -> dvString(name))
                  ),
                  "value" -> schemaToDynamicValue(schema)
                )
              ),
              "required" -> dvSequence(
                ArraySeq(
                  dvString("tag"),
                  dvString("value")
                )
              )
            )
          )
        }.toSeq
        dvRecord(
          "oneOf"            -> dvSequence(ArraySeq.from(oneOf)),
          config.nodeTypeKey -> dvString("variant"),
          config.typeNameKey -> dvString(v.typeName.toString)
        )
      case o: `oneof`[?] =>
        val alternatives = o.alternatives.map(schemaToDynamicValue)
        dvRecord(
          "oneOf" -> DynamicValue.Sequence(ArraySeq.from(alternatives))
        )
    }
}
