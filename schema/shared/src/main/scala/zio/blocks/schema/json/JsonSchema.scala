package zio.blocks.schema
package json

import binding.*
import scala.collection.immutable.ArraySeq
import scala.collection.mutable

/**
 * JsonSchema AST representation with support for bindings, validations, and
 * custom configurations. Inspired by andyglow/scala-jsonschema but tailored for
 * ZIO Blocks.
 */
sealed trait JsonSchema[+T] {
  def jsonType: String
  def title: Option[String]              = None
  def description: Option[String]        = None
  def zioValidations: Seq[ZioValidation] = Seq.empty
  def zioModifiers: Seq[ZioModifier]     = Seq.empty
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
  validationAsGrammar: Boolean = false
) {
  val nodeTypeKey: String = s"$namespacePrefix:nodeType"
  val typeNameKey: String = s"$namespacePrefix:typeName"
  val primitiveKey: String = s"$namespacePrefix:primitive"
  val keyKey: String = s"$namespacePrefix:key"
  val wrappedKey: String = s"$namespacePrefix:wrapped"
}

object JsonSchemaConfig {
  implicit val default: JsonSchemaConfig = JsonSchemaConfig()
  val minimal: JsonSchemaConfig          = JsonSchemaConfig(
    includeZioExtensions = false,
    includeValidations = false,
    includeBindings = false,
    includeModifiers = false,
    includeExamples = false,
    includeDefaults = false
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
    properties: Map[String, JsonSchema[?]],
    required: Seq[String] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = "object"
  }

  case class `oneof`[T](alternatives: Seq[JsonSchema[?]]) extends JsonSchema[T] {
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
    typeName: String,
    fields: Map[String, JsonSchema[?]],
    required: Seq[String] = Seq.empty,
    override val title: Option[String] = None,
    override val description: Option[String] = None,
    override val zioModifiers: Seq[ZioModifier] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = "object"
  }

  case class `zio-variant`[T](
    typeName: String,
    cases: Map[String, JsonSchema[?]],
    override val title: Option[String] = None,
    override val description: Option[String] = None,
    override val zioModifiers: Seq[ZioModifier] = Seq.empty
  ) extends JsonSchema[T] {
    override def jsonType: String = "oneOf"
  }

  // Conversion methods
  def fromReflect[F[_, _], A](reflect: Reflect[F, A])(implicit config: JsonSchemaConfig): JsonSchema[A] =
    (reflect: Any) match {
      case p: Reflect.Primitive[?, ?]   => fromPrimitive(p.asInstanceOf[Reflect.Primitive[F, A]])
      case r: Reflect.Record[?, ?]      => fromRecord(r.asInstanceOf[Reflect.Record[F, A]])
      case v: Reflect.Variant[?, ?]     => fromVariant(v.asInstanceOf[Reflect.Variant[F, A]])
      case s: Reflect.Sequence[?, ?, _] => fromSequence(s, config).asInstanceOf[JsonSchema[A]]
      case m: Reflect.Map[?, ?, ?, ?]   => fromMap(m, config).asInstanceOf[JsonSchema[A]]
      case d: Reflect.Dynamic[?]        => fromDynamic(d.asInstanceOf[Reflect.Dynamic[F]], config).asInstanceOf[JsonSchema[A]]
      case w: Reflect.Wrapper[?, ?, ?]  => fromWrapper(w.asInstanceOf[Reflect.Wrapper[F, A, Any]])(config)
      case d: Reflect.Deferred[?, ?]    => fromReflect(d.value.asInstanceOf[Reflect[F, A]])(config)
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

  private def fromRecord[F[_, _], A](rec: Reflect.Record[F, A])(implicit config: JsonSchemaConfig): JsonSchema[A] = {
    val properties = rec.fields.map { f =>
      f.name -> fromReflect(f.value)(config)
    }.toMap

    val required = rec.fields.map(_.name).toSeq

    if (config.includeZioExtensions) {
      `zio-record`[A](
        typeName = rec.typeName.name,
        fields = properties,
        required = required,
        title = Some(rec.typeName.name),
        description = if (rec.doc != Doc.Empty) Some(docToString(rec.doc)) else None,
        zioModifiers = if (config.includeModifiers) extractModifiers(rec.modifiers) else Seq.empty
      )
    } else {
      `object`[A](properties, required)
    }
  }

  private def fromVariant[F[_, _], A](
    variant: Reflect.Variant[F, A]
  )(implicit config: JsonSchemaConfig): JsonSchema[A] = {
    val cases = variant.cases.map { c =>
      c.name -> fromReflect(c.value)(config)
    }.toMap

    if (config.includeZioExtensions) {
      `zio-variant`[A](
        typeName = variant.typeName.name,
        cases = cases,
        title = Some(variant.typeName.name),
        description = if (variant.doc != Doc.Empty) Some(docToString(variant.doc)) else None,
        zioModifiers = if (config.includeModifiers) extractModifiers(variant.modifiers) else Seq.empty
      )
    } else {
      val alternatives = variant.cases.map { c =>
        `object`[A](
          properties = Map(
            "tag"   -> `string`(),
            "value" -> fromReflect(c.value)(config)
          ),
          required = Seq("tag", "value")
        )
      }
      `oneof`[A](alternatives)
    }
  }

  private def fromSequence[F[_, _], A, C[_]](
    seq: Reflect.Sequence[F, A, C],
    config: JsonSchemaConfig
  ): JsonSchema[C[A]] = {
    val elementSchema = fromReflect(seq.element)(config)
    `array`(elementSchema).asInstanceOf[JsonSchema[C[A]]]
  }

  private def fromMap[F[_, _], K, V, M[_, _]](
    map: Reflect.Map[F, K, V, M],
    config: JsonSchemaConfig
  ): JsonSchema[M[K, V]] = {
    val valueSchema = fromReflect(map.value)(config)
    `object`[M[K, V]](
      properties = Map.empty, // Maps use additionalProperties instead
      required = Seq.empty
    )
  }

  private def fromDynamic[F[_, _]](dyn: Reflect.Dynamic[F], config: JsonSchemaConfig): JsonSchema[DynamicValue] =
    `object`[DynamicValue](
      properties = Map.empty,
      required = Seq.empty
    )

  private def fromWrapper[F[_, _], A, B](wrap: Reflect.Wrapper[F, A, B])(implicit
    config: JsonSchemaConfig
  ): JsonSchema[A] =
    fromReflect(wrap.wrapped)(config).asInstanceOf[JsonSchema[A]]

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
    modifiers.map {
      case Modifier.config(k, v) => ZioModifier("config", Some(k), Some(v), None)
      case _: Modifier.transient => ZioModifier("transient", None, None, None)
      case _                     => ZioModifier("unknown", None, None, None)
    }

  /**
   * Convert a Reflect to a proper JSON Schema DynamicValue. The result will be
   * a valid JSON Schema that can be serialized to JSON.
   */
  def toJsonSchema[F[_, _], A](r: Reflect[F, A]): DynamicValue = {
    val ctx  = new GenCtx(mutable.LinkedHashMap.empty[String, DynamicValue], mutable.HashSet.empty[String])
    val root = reflectToJsonSchemaDynamicWithCtx(r, JsonSchemaConfig.default, ctx)
    if (ctx.defs.isEmpty) root
    else
      root match {
        case DynamicValue.Record(fields) =>
          val defsRecord = DynamicValue.Record(ArraySeq.from(ctx.defs.toSeq))
          DynamicValue.Record(fields :+ ("$defs" -> defsRecord))
        case other =>
          // Wrap non-record root with $defs (represent root via $ref -> body)
          val defsRecord = DynamicValue.Record(ArraySeq.from(ctx.defs.toSeq))
          DynamicValue.Record(
            ArraySeq(
              "$ref"  -> other,
              "$defs" -> defsRecord
            )
          )
      }
  }

  /**
   * Convert a JSON Schema DynamicValue back to an unbound Reflect. This allows
   * deserializing schemas from JSON Schema format.
   */
  def fromJsonSchema(value: DynamicValue): Either[SchemaError, Reflect[NoBinding, Any]] =
    dynamicValueToReflect(value)

  // Context for $defs/$ref emission and cycle detection
  private final case class GenCtx(
    defs: mutable.LinkedHashMap[String, DynamicValue],
    visiting: mutable.HashSet[String]
  )

  private def defKey(tn: TypeName[?]): String = typeNameToString(tn)

  private def makeRef(key: String): DynamicValue =
    DynamicValue.Record(ArraySeq("$ref" -> dvString(s"#/$$defs/$key")))

  private def reflectToJsonSchemaDynamicWithCtx[F[_, _], A](
    reflect: Reflect[F, A],
    config: JsonSchemaConfig,
    ctx: GenCtx
  ): DynamicValue = {
    reflect match {
      case p: Reflect.Primitive[F, A] =>
        val prim = p
        val key  = defKey(prim.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
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
            fields += s"${config.zioNodeTypePrefix}:nodeType"  -> dvString("primitive")
            fields += s"${config.zioNodeTypePrefix}:primitive" -> dvString(prim.primitiveType.typeName.name)
            fields += s"${config.zioNodeTypePrefix}:typeName"  -> dvString(typeNameToString(prim.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case r: Reflect.Record[_, _] =>
        val rec = r.asInstanceOf[Reflect.Record[F, A]]
        val key = defKey(rec.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val props = rec.fields.map { f =>
            f.name -> reflectToJsonSchemaDynamicWithCtx(f.value, config, ctx)
          }
          val required = rec.fields.map(_.name)
          val fields   = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          fields += "properties"                            -> DynamicValue.Record(ArraySeq.from(props))
          fields += "required"                              -> DynamicValue.Sequence(ArraySeq.from(required.map(dvString)))
          if (rec.doc != Doc.Empty) fields += "description" -> dvString(docToString(rec.doc))
          if (config.includeZioExtensions) {
            fields += s"${config.zioNodeTypePrefix}:nodeType" -> dvString("record")
            fields += s"${config.zioNodeTypePrefix}:typeName" -> dvString(typeNameToString(rec.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case v: Reflect.Variant[_, _] =>
        val variant = v.asInstanceOf[Reflect.Variant[F, A]]
        val key     = defKey(variant.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val oneOf = variant.cases.map { c =>
            DynamicValue.Record(
              ArraySeq(
                "type"       -> dvString("object"),
                "properties" -> DynamicValue.Record(
                  ArraySeq(
                    "tag"   -> DynamicValue.Record(ArraySeq("const" -> dvString(c.name))),
                    "value" -> reflectToJsonSchemaDynamicWithCtx(c.value, config, ctx)
                  )
                ),
                "required" -> DynamicValue.Sequence(ArraySeq(dvString("tag"), dvString("value")))
              )
            )
          }
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "oneOf"                                     -> DynamicValue.Sequence(ArraySeq.from(oneOf))
          if (variant.doc != Doc.Empty) fields += "description" -> dvString(docToString(variant.doc))
          if (config.includeZioExtensions) {
            fields += s"${config.zioNodeTypePrefix}:nodeType" -> dvString("variant")
            fields += s"${config.zioNodeTypePrefix}:typeName" -> dvString(typeNameToString(variant.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case s: Reflect.Sequence[_, _, _] =>
        val seq = s.asInstanceOf[Reflect.Sequence[F, Any, List]]
        val key = defKey(seq.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("array")
          fields += "items"                                 -> reflectToJsonSchemaDynamicWithCtx(seq.element, config, ctx)
          if (seq.doc != Doc.Empty) fields += "description" -> dvString(docToString(seq.doc))
          if (config.includeZioExtensions) {
            fields += "zio:nodeType" -> dvString("sequence")
            fields += "zio:typeName" -> dvString(typeNameToString(seq.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case m: Reflect.Map[_, _, _, _] =>
        val map     = m.asInstanceOf[Reflect.Map[F, Any, Any, scala.collection.immutable.Map]]
        val keyName = defKey(map.typeName)
        if (ctx.defs.contains(keyName) || ctx.visiting.contains(keyName)) makeRef(keyName)
        else {
          ctx.visiting += keyName
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          fields += "additionalProperties"                  -> reflectToJsonSchemaDynamicWithCtx(map.value, config, ctx)
          if (map.doc != Doc.Empty) fields += "description" -> dvString(docToString(map.doc))
          if (config.includeZioExtensions) {
            fields += s"${config.zioNodeTypePrefix}:nodeType" -> dvString("map")
            fields += s"${config.zioNodeTypePrefix}:typeName" -> dvString(typeNameToString(map.typeName))
            fields += s"${config.zioNodeTypePrefix}:key"      -> reflectToJsonSchemaDynamicWithCtx(map.key, config, ctx)
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= keyName
          ctx.defs.put(keyName, body)
          makeRef(keyName)
        }

      case d: Reflect.Dynamic[?] =>
        val dyn = d.asInstanceOf[Reflect.Dynamic[F]]
        val key = defKey(dyn.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val fields = ArraySeq.newBuilder[(String, DynamicValue)]
          fields += "type"                                  -> dvString("object")
          if (dyn.doc != Doc.Empty) fields += "description" -> dvString(docToString(dyn.doc))
          if (config.includeZioExtensions) {
            fields += s"${config.zioNodeTypePrefix}:nodeType" -> dvString("dynamic")
            fields += s"${config.zioNodeTypePrefix}:typeName" -> dvString(typeNameToString(dyn.typeName))
          }
          val body = DynamicValue.Record(fields.result())
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case w: Reflect.Wrapper[_, _, _] =>
        val wrap = w.asInstanceOf[Reflect.Wrapper[F, A, Any]]
        val key  = defKey(wrap.typeName)
        if (ctx.defs.contains(key) || ctx.visiting.contains(key)) makeRef(key)
        else {
          ctx.visiting += key
          val wrappedSchema = reflectToJsonSchemaDynamicWithCtx(wrap.wrapped, config, ctx)
          val body          = if (config.includeZioExtensions) {
            val fields = ArraySeq.newBuilder[(String, DynamicValue)]
            wrappedSchema match {
              case DynamicValue.Record(wrappedFields) => fields ++= wrappedFields
              case _                                  => fields += "type" -> dvString("object")
            }
            fields += "zio:nodeType"                           -> dvString("wrapper")
            fields += "zio:typeName"                           -> dvString(typeNameToString(wrap.typeName))
            fields += "zio:wrapped"                            -> wrappedSchema
            if (wrap.doc != Doc.Empty) fields += "description" -> dvString(docToString(wrap.doc))
            DynamicValue.Record(fields.result())
          } else wrappedSchema
          ctx.visiting -= key
          ctx.defs.put(key, body)
          makeRef(key)
        }

      case d: Reflect.Deferred[_, _] =>
        reflectToJsonSchemaDynamicWithCtx(d.value, config, ctx)
    }
  }

  private def dynamicValueToReflect(value: DynamicValue): Either[SchemaError, Reflect[NoBinding, Any]] =
    // Implementation for converting JSON Schema DynamicValue back to Reflect
    // This is a placeholder - full implementation would parse all JSON Schema constructs
    Left(SchemaError.invalidType(Nil, "fromJsonSchema implementation in progress"))

  // Helper methods for DynamicValue creation
  private def dvString(s: String): DynamicValue     = DynamicValue.Primitive(PrimitiveValue.String(s))
  private def dvInt(i: Int): DynamicValue           = DynamicValue.Primitive(PrimitiveValue.Int(i))
  private def dvNumber(n: BigDecimal): DynamicValue = DynamicValue.Primitive(PrimitiveValue.BigDecimal(n))
  private def dvBoolean(b: Boolean): DynamicValue   = DynamicValue.Primitive(PrimitiveValue.Boolean(b))

  private def typeNameToString(tn: TypeName[?]): String = {
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

  private def schemaToDynamicValue(schema: JsonSchema[?])(implicit config: JsonSchemaConfig): DynamicValue =
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
        DynamicValue.Record(
          ArraySeq(
            "type"       -> DynamicValue.Primitive(PrimitiveValue.String("object")),
            "properties" -> DynamicValue.Record(ArraySeq.from(props)),
            "required"   -> DynamicValue.Sequence(
              ArraySeq.from(o.required.map(s => DynamicValue.Primitive(PrimitiveValue.String(s))))
            )
          )
        )
      case p: `zio-primitive`[?] =>
        val fields = ArraySeq.newBuilder[(String, DynamicValue)]
        fields += "type"          -> DynamicValue.Primitive(PrimitiveValue.String(p.jsonType))
        fields += s"${config.zioNodeTypePrefix}:nodeType"  -> DynamicValue.Primitive(PrimitiveValue.String("primitive"))
        fields += s"${config.zioNodeTypePrefix}:primitive" -> DynamicValue.Primitive(PrimitiveValue.String(p.primitiveType))
        p.title.foreach(t => fields += "title" -> DynamicValue.Primitive(PrimitiveValue.String(t)))
        p.description.foreach(d => fields += "description" -> DynamicValue.Primitive(PrimitiveValue.String(d)))
        DynamicValue.Record(fields.result())
      case r: `zio-record`[?] =>
        val props = r.fields.map { case (name, schema) =>
          name -> schemaToDynamicValue(schema)
        }
        DynamicValue.Record(
          ArraySeq(
            "type"         -> DynamicValue.Primitive(PrimitiveValue.String("object")),
            s"${config.zioNodeTypePrefix}:nodeType" -> DynamicValue.Primitive(PrimitiveValue.String("record")),
            s"${config.zioNodeTypePrefix}:typeName" -> DynamicValue.Primitive(PrimitiveValue.String(r.typeName)),
            "properties"   -> DynamicValue.Record(ArraySeq.from(props)),
            "required"     -> DynamicValue.Sequence(
              ArraySeq.from(r.required.map(s => DynamicValue.Primitive(PrimitiveValue.String(s))))
            )
          )
        )
      case v: `zio-variant`[?] =>
        val oneOf = v.cases.map { case (name, schema) =>
          DynamicValue.Record(
            ArraySeq(
              "type"       -> DynamicValue.Primitive(PrimitiveValue.String("object")),
              "properties" -> DynamicValue.Record(
                ArraySeq(
                  "tag" -> DynamicValue.Record(
                    ArraySeq("const" -> DynamicValue.Primitive(PrimitiveValue.String(name)))
                  ),
                  "value" -> schemaToDynamicValue(schema)
                )
              ),
              "required" -> DynamicValue.Sequence(
                ArraySeq(
                  DynamicValue.Primitive(PrimitiveValue.String("tag")),
                  DynamicValue.Primitive(PrimitiveValue.String("value"))
                )
              )
            )
          )
        }.toSeq
        DynamicValue.Record(
          ArraySeq(
            "oneOf"        -> DynamicValue.Sequence(ArraySeq.from(oneOf)),
            s"${config.zioNodeTypePrefix}:nodeType" -> DynamicValue.Primitive(PrimitiveValue.String("variant")),
            s"${config.zioNodeTypePrefix}:typeName" -> DynamicValue.Primitive(PrimitiveValue.String(v.typeName))
          )
        )
      case o: `oneof`[?] =>
        val alternatives = o.alternatives.map(schemaToDynamicValue)
        DynamicValue.Record(
          ArraySeq(
            "oneOf" -> DynamicValue.Sequence(ArraySeq.from(alternatives))
          )
        )
    }
}
