package zio.blocks.schema.json

import zio.blocks.schema.*
import zio.blocks.schema.binding.*
import zio.blocks.schema.codec.*
import zio.blocks.schema.derive.*
import zio.blocks.schema.validation.*

object JsonSchema {
  def toJsonSchema[F[_, _], A](r: Reflect[F, A]): DynamicValue = (r: Any) match {
    case p: Reflect.Primitive[_, _] =>
      val prim = p.asInstanceOf[Reflect.Primitive[F, Any]]
      DynamicValue.Record(
        ArraySeq(
          "type"          -> dvString(jsonType(prim.primitiveType)),
          "zio:nodeType"  -> dvString("primitive"),
          "zio:primitive" -> dvString(prim.primitiveType.typeName.name),
          "zio:typeName"  -> typeNameToDynamic(prim.typeName),
          "zio:doc"       -> docToDynamic(prim.doc),
          "zio:modifiers" -> modifiersToDynamic(prim.modifiers)
        )
      )
    case r0: Reflect.Record[_, _] =>
      val rec   = r0.asInstanceOf[Reflect.Record[F, Any]]
      val props = rec.fields.map { f =>
        f.name -> f.value.toJsonSchema
      }
      val zioFields = rec.fields.map { f =>
        DynamicValue.Record(
          ArraySeq(
            "name"          -> dvString(f.name),
            "schema"        -> f.value.toJsonSchema,
            "zio:doc"       -> docToDynamic(f.doc),
            "zio:modifiers" -> modifiersToDynamic(f.modifiers)
          )
        )
      }
      DynamicValue.Record(
        ArraySeq(
          "type"          -> dvString("object"),
          "zio:nodeType"  -> dvString("record"),
          "zio:typeName"  -> typeNameToDynamic(rec.typeName),
          "properties"    -> DynamicValue.Record(ArraySeq.from(props)),
          "required"      -> DynamicValue.Sequence(ArraySeq.from(rec.fields.map(f => dvString(f.name)))),
          "zio:fields"    -> DynamicValue.Sequence(ArraySeq.from(zioFields)),
          "zio:doc"       -> docToDynamic(rec.doc),
          "zio:modifiers" -> modifiersToDynamic(rec.modifiers)
        )
      )
    case v0: Reflect.Variant[_, _] =>
      val v     = v0.asInstanceOf[Reflect.Variant[F, Any]]
      val oneOf = v.cases.map { c =>
        DynamicValue.Record(
          ArraySeq(
            "type"       -> dvString("object"),
            "properties" -> DynamicValue.Record(
              ArraySeq(
                "tag"   -> DynamicValue.Record(ArraySeq("const" -> dvString(c.name))),
                "value" -> c.value.toJsonSchema
              )
            ),
            "required" -> DynamicValue.Sequence(ArraySeq(dvString("tag"), dvString("value")))
          )
        )
      }
      val zioCases = v.cases.map { c =>
        DynamicValue.Record(
          ArraySeq(
            "name"          -> dvString(c.name),
            "schema"        -> c.value.toJsonSchema,
            "zio:doc"       -> docToDynamic(c.doc),
            "zio:modifiers" -> modifiersToDynamic(c.modifiers)
          )
        )
      }
      DynamicValue.Record(
        ArraySeq(
          "oneOf"         -> DynamicValue.Sequence(ArraySeq.from(oneOf)),
          "zio:nodeType"  -> dvString("variant"),
          "zio:typeName"  -> typeNameToDynamic(v.typeName),
          "zio:cases"     -> DynamicValue.Sequence(ArraySeq.from(zioCases)),
          "zio:doc"       -> docToDynamic(v.doc),
          "zio:modifiers" -> modifiersToDynamic(v.modifiers)
        )
      )
    case s0: Reflect.Sequence[_, _, _] =>
      val s = s0.asInstanceOf[Reflect.Sequence[F, Any, List]]
      DynamicValue.Record(
        ArraySeq(
          "type"          -> dvString("array"),
          "items"         -> s.element.toJsonSchema,
          "zio:nodeType"  -> dvString("sequence"),
          "zio:typeName"  -> typeNameToDynamic(s.typeName),
          "zio:doc"       -> docToDynamic(s.doc),
          "zio:modifiers" -> modifiersToDynamic(s.modifiers)
        )
      )
    case m0: Reflect.Map[_, _, _, _] =>
      val m = m0.asInstanceOf[Reflect.Map[F, Any, Any, scala.collection.immutable.Map]]
      DynamicValue.Record(
        ArraySeq(
          "type"                 -> dvString("object"),
          "additionalProperties" -> m.value.toJsonSchema,
          "zio:key"              -> m.key.toJsonSchema,
          "zio:nodeType"         -> dvString("map"),
          "zio:typeName"         -> typeNameToDynamic(m.typeName),
          "zio:doc"              -> docToDynamic(m.doc),
          "zio:modifiers"        -> modifiersToDynamic(m.modifiers)
        )
      )
    case d0: Reflect.Dynamic[_] =>
      val d = d0.asInstanceOf[Reflect.Dynamic[F]]
      DynamicValue.Record(
        ArraySeq(
          "zio:nodeType"  -> dvString("dynamic"),
          "zio:typeName"  -> typeNameToDynamic(d.typeName),
          "zio:doc"       -> docToDynamic(d.doc),
          "zio:modifiers" -> modifiersToDynamic(d.modifiers)
        )
      )
    case w0: Reflect.Wrapper[_, _, _] =>
      val w = w0.asInstanceOf[Reflect.Wrapper[F, Any, Any]]
      DynamicValue.Record(
        ArraySeq(
          "zio:nodeType"  -> dvString("wrapper"),
          "zio:typeName"  -> typeNameToDynamic(w.typeName),
          "zio:wrapped"   -> w.wrapped.toJsonSchema,
          "zio:doc"       -> docToDynamic(w.doc),
          "zio:modifiers" -> modifiersToDynamic(w.modifiers)
        )
      )
    case d: Reflect.Deferred[_, _] => toJsonSchema(d.value)
  }

  def fromJsonSchema(value: DynamicValue): Either[SchemaError, Reflect[NoBinding, Any]] = value match {
    case DynamicValue.Record(fields) =>
      val map = recordToMap(fields)
      map.get("zio:nodeType") match {
        case Some(DynamicValue.Primitive(PrimitiveValue.String("primitive"))) =>
          val pname = getString(map("zio:primitive"))
          val pt    = primitiveTypeFromName(pname)
          val tn    = map.get("zio:typeName").map(dynamicToTypeName).getOrElse(TypeName.unit).asInstanceOf[TypeName[Any]]
          val doc   = dynamicToDoc(map.get("zio:doc"))
          val mods  = dynamicToModifiers(map.get("zio:modifiers"))
          Right(
            Reflect.Primitive(
              pt.asInstanceOf[PrimitiveType[Any]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Primitive]]
            ).asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("record"))) =>
          val tn        = map.get("zio:typeName").map(dynamicToTypeName).getOrElse(TypeName.unit).asInstanceOf[TypeName[Any]]
          val doc       = dynamicToDoc(map.get("zio:doc"))
          val mods      = dynamicToModifiers(map.get("zio:modifiers"))
          val fieldsSeq = map.get("zio:fields") match {
            case Some(seq: DynamicValue.Sequence) => seq.elements
            case _                                => IndexedSeq.empty
          }
          val terms = fieldsSeq.map {
            case DynamicValue.Record(fmap) =>
              val fm     = recordToMap(fmap)
              val name   = getString(fm("name"))
              val schema = fromJsonSchema(fm("schema")).toOption.get
              val fdoc   = dynamicToDoc(fm.get("zio:doc"))
              val fmods  = dynamicToModifiers(fm.get("zio:modifiers"))
              new Term[NoBinding, Any, Any](
                name,
                schema.asInstanceOf[Reflect[NoBinding, Any]],
                fdoc,
                fmods.asInstanceOf[Seq[Modifier.Term]]
              )
            case _ => new Term[NoBinding, Any, Any]("", dynamicNothing)
          }
          Right(
            Reflect.Record(
              terms.asInstanceOf[IndexedSeq[Term[NoBinding, Any, ?]]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Record]]
            )
              .asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("variant"))) =>
          val tn       = map.get("zio:typeName").map(dynamicToTypeName).getOrElse(TypeName.unit).asInstanceOf[TypeName[Any]]
          val doc      = dynamicToDoc(map.get("zio:doc"))
          val mods     = dynamicToModifiers(map.get("zio:modifiers"))
          val casesSeq = map.get("zio:cases") match {
            case Some(seq: DynamicValue.Sequence) => seq.elements
            case _                                => IndexedSeq.empty
          }
          val terms = casesSeq.map {
            case DynamicValue.Record(fmap) =>
              val fm     = recordToMap(fmap)
              val name   = getString(fm("name"))
              val schema = fromJsonSchema(fm("schema")).toOption.get
              val fdoc   = dynamicToDoc(fm.get("zio:doc"))
              val fmods  = dynamicToModifiers(fm.get("zio:modifiers"))
              new Term[NoBinding, Any, Any](
                name,
                schema.asInstanceOf[Reflect[NoBinding, Any]],
                fdoc,
                fmods.asInstanceOf[Seq[Modifier.Term]]
              )
            case _ => new Term[NoBinding, Any, Any]("", dynamicNothing)
          }
          Right(
            Reflect.Variant(
              terms.asInstanceOf[IndexedSeq[Term[NoBinding, Any, ? <: Any]]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Variant]]
            )
              .asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("sequence"))) =>
          val tn =
            map.get("zio:typeName").map(dynamicToTypeName).getOrElse(TypeName.unit).asInstanceOf[TypeName[List[Any]]]
          val doc  = dynamicToDoc(map.get("zio:doc"))
          val mods = dynamicToModifiers(map.get("zio:modifiers"))
          val elem = map
            .get("items")
            .map(v => fromJsonSchema(v).toOption.get)
            .getOrElse(dynamicNothing)
          Right(
            Reflect.Sequence(
              elem.asInstanceOf[Reflect[NoBinding, Any]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Seq]]
            )
              .asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("map"))) =>
          val tn = map
            .get("zio:typeName")
            .map(dynamicToTypeName)
            .getOrElse(TypeName.unit)
            .asInstanceOf[TypeName[scala.collection.immutable.Map[Any, Any]]]
          val doc  = dynamicToDoc(map.get("zio:doc"))
          val mods = dynamicToModifiers(map.get("zio:modifiers"))
          val key  = map
            .get("zio:key")
            .map(v => fromJsonSchema(v).toOption.get)
            .getOrElse(dynamicNothing)
          val valueSchema = map
            .get("additionalProperties")
            .map(v => fromJsonSchema(v).toOption.get)
            .getOrElse(dynamicNothing)
          Right(
            Reflect.Map(
              key.asInstanceOf[Reflect[NoBinding, Any]],
              valueSchema.asInstanceOf[Reflect[NoBinding, Any]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Map]]
            )
              .asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("dynamic"))) =>
          val doc  = dynamicToDoc(map.get("zio:doc"))
          val mods = dynamicToModifiers(map.get("zio:modifiers"))
          Right(
            Reflect.Dynamic(NoBinding(), doc, mods.asInstanceOf[Seq[Modifier.Dynamic]]).asInstanceOf[Reflect[NoBinding, Any]]
          )
        case Some(DynamicValue.Primitive(PrimitiveValue.String("wrapper"))) =>
          val tn      = map.get("zio:typeName").map(dynamicToTypeName).getOrElse(TypeName.unit).asInstanceOf[TypeName[Any]]
          val doc     = dynamicToDoc(map.get("zio:doc"))
          val mods    = dynamicToModifiers(map.get("zio:modifiers"))
          val wrapped = map
            .get("zio:wrapped")
            .map(v => fromJsonSchema(v).toOption.get)
            .getOrElse(dynamicNothing)
          Right(
            Reflect.Wrapper(
              wrapped.asInstanceOf[Reflect[NoBinding, Any]],
              tn,
              NoBinding(),
              doc,
              mods.asInstanceOf[Seq[Modifier.Wrapper]]
            ).asInstanceOf[Reflect[NoBinding, Any]]
          )
        case _ => Left(SchemaError.invalidType(Nil, "Unknown node type"))
      }
    case _ => Left(SchemaError.invalidType(Nil, "Expected record"))
  }

  private def recordToMap(
    fields: IndexedSeq[(String, DynamicValue)]
  ): scala.collection.immutable.Map[String, DynamicValue] =
    fields.iterator.map(kv => kv._1 -> kv._2).toMap

  private def dvString(s: String): DynamicValue = new DynamicValue.Primitive(new PrimitiveValue.String(s))

  private def docToDynamic(doc: Doc): DynamicValue = doc match {
    case Doc.Empty => dvString("")
    case _         =>
      val sb = new StringBuilder
      doc.flatten.foreach { case Doc.Text(v) => sb.append(v); case _ => () }
      dvString(sb.toString)
  }

  private def dynamicToDoc(value: Option[DynamicValue]): Doc = value match {
    case Some(DynamicValue.Primitive(PrimitiveValue.String(s))) => if (s.isEmpty) Doc.Empty else Doc.Text(s)
    case _                                                      => Doc.Empty
  }

  private def modifiersToDynamic(mods: Seq[Modifier]): DynamicValue = {
    val elems = mods.map {
      case Modifier.config(k, v) =>
        DynamicValue.Record(ArraySeq("type" -> dvString("config"), "key" -> dvString(k), "value" -> dvString(v)))
      case _: Modifier.transient => DynamicValue.Record(ArraySeq("type" -> dvString("transient")))
      case null                  => DynamicValue.Record(ArraySeq("type" -> dvString("unknown")))
    }
    DynamicValue.Sequence(ArraySeq.from(elems))
  }

  private def dynamicToModifiers(value: Option[DynamicValue]): Seq[Modifier] = value match {
    case Some(seq: DynamicValue.Sequence) =>
      seq.elements.flatMap {
        case DynamicValue.Record(flds) =>
          val fm = recordToMap(flds)
          getString(fm.getOrElse("type", dvString(""))) match {
            case "config" =>
              val k = getString(fm.getOrElse("key", dvString("")))
              val v = getString(fm.getOrElse("value", dvString("")))
              Some(Modifier.config(k, v))
            case "transient" => Some(Modifier.transient())
            case _           => None
          }
        case _ => None
      }
    case _ => Nil
  }

  private def typeNameToDynamic(tn: TypeName[?]): DynamicValue = {
    val nsElems = tn.namespace.elements.map(dvString)
    val params  = tn.params.map(typeNameToDynamic)
    DynamicValue.Record(
      ArraySeq(
        "name"      -> dvString(tn.name),
        "namespace" -> DynamicValue.Sequence(ArraySeq.from(nsElems)),
        "params"    -> DynamicValue.Sequence(ArraySeq.from(params))
      )
    )
  }

  private def dynamicToTypeName(value: DynamicValue): TypeName[Any] = value match {
    case DynamicValue.Record(fields) =>
      val m         = recordToMap(fields)
      val name      = getString(m.getOrElse("name", dvString("")))
      val namespace = m.get("namespace") match {
        case Some(seq: DynamicValue.Sequence) =>
          Namespace(seq.elements.collect { case DynamicValue.Primitive(PrimitiveValue.String(s)) => s })
        case _ => Namespace(Nil)
      }
      val params = m.get("params") match {
        case Some(seq: DynamicValue.Sequence) => seq.elements.map(dynamicToTypeName)
        case _                                => IndexedSeq.empty
      }
      TypeName[Any](namespace, name, params)
    case _ => TypeName.unit.asInstanceOf[TypeName[Any]]
  }

  private def getString(dv: DynamicValue): String = dv match {
    case DynamicValue.Primitive(PrimitiveValue.String(s)) => s
    case _                                                => ""
  }

  private val dynamicNothing: Reflect[NoBinding, Any] = Reflect.Primitive(
    PrimitiveType.Unit.asInstanceOf[PrimitiveType[Any]],
    TypeName.unit.asInstanceOf[TypeName[Any]],
    NoBinding()
  )

  private def jsonType(pt: PrimitiveType[?]): String = pt match {
    case PrimitiveType.Unit | _: PrimitiveType.Unit.type => "null"
    case _: PrimitiveType.Boolean                        => "boolean"
    case _: PrimitiveType.Byte | _: PrimitiveType.Short | _: PrimitiveType.Int | _: PrimitiveType.Long |
        _: PrimitiveType.BigInt =>
      "integer"
    case _: PrimitiveType.Float | _: PrimitiveType.Double | _: PrimitiveType.BigDecimal => "number"
    case _                                                                              => "string"
  }

  private def primitiveTypeFromName(name: String): PrimitiveType[Any] = name match {
    case "Unit"           => PrimitiveType.Unit.asInstanceOf[PrimitiveType[Any]]
    case "Boolean"        => PrimitiveType.Boolean(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Byte"           => PrimitiveType.Byte(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Short"          => PrimitiveType.Short(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Int"            => PrimitiveType.Int(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Long"           => PrimitiveType.Long(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Float"          => PrimitiveType.Float(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Double"         => PrimitiveType.Double(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Char"           => PrimitiveType.Char(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "String"         => PrimitiveType.String(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "BigInt"         => PrimitiveType.BigInt(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "BigDecimal"     => PrimitiveType.BigDecimal(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "DayOfWeek"      => PrimitiveType.DayOfWeek(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Duration"       => PrimitiveType.Duration(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Instant"        => PrimitiveType.Instant(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "LocalDate"      => PrimitiveType.LocalDate(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "LocalDateTime"  => PrimitiveType.LocalDateTime(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "LocalTime"      => PrimitiveType.LocalTime(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Month"          => PrimitiveType.Month(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "MonthDay"       => PrimitiveType.MonthDay(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "OffsetDateTime" => PrimitiveType.OffsetDateTime(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "OffsetTime"     => PrimitiveType.OffsetTime(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Period"         => PrimitiveType.Period(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Year"           => PrimitiveType.Year(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "YearMonth"      => PrimitiveType.YearMonth(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "ZoneId"         => PrimitiveType.ZoneId(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "ZoneOffset"     => PrimitiveType.ZoneOffset(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "ZonedDateTime"  => PrimitiveType.ZonedDateTime(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "Currency"       => PrimitiveType.Currency(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case "UUID"           => PrimitiveType.UUID(Validation.None).asInstanceOf[PrimitiveType[Any]]
    case _                => PrimitiveType.String(Validation.None).asInstanceOf[PrimitiveType[Any]]
  }
}
