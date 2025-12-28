package zio.blocks.schema

import scala.collection.mutable
import scala.quoted.*

trait TypeIdVersionSpecific {
  inline def derive[A]: TypeId.OfType = ${ TypeIdVersionSpecificImpl.derive[A] }
}

private object TypeIdVersionSpecificImpl {
  def derive[A: Type](using Quotes): Expr[TypeId.OfType] =
    new TypeIdVersionSpecificImpl().derive[A]
}

private final class TypeIdVersionSpecificImpl(using Quotes) {
  import quotes.reflect.*

  private val anyTpe = defn.AnyClass.typeRef

  private val cache = new mutable.HashMap[TypeRepr, TypeIdRepr]

  private def isUnion(tpe: TypeRepr): Boolean = CommonMacroOps.isUnion(tpe)

  private def allUnionTypes(tpe: TypeRepr): List[TypeRepr] = CommonMacroOps.allUnionTypes(tpe)

  private def typeArgs(tpe: TypeRepr): List[TypeRepr] = CommonMacroOps.typeArgs(tpe)

  private def isGenericTuple(tpe: TypeRepr): Boolean = CommonMacroOps.isGenericTuple(tpe)

  private def genericTupleTypeArgs(tpe: TypeRepr): List[TypeRepr] = CommonMacroOps.genericTupleTypeArgs(tpe)

  private def isNamedTuple(tpe: TypeRepr): Boolean = tpe match {
    case AppliedType(ntTpe, _) => ntTpe.typeSymbol.fullName == "scala.NamedTuple$.NamedTuple"
    case _                     => false
  }

  private def ownerAndName(tpe: TypeRepr): (Owner, String) = {
    var packages: List[String] = Nil
    var values: List[String]   = Nil
    var name: String           = null

    val tpeTypeSymbol = tpe.typeSymbol
    name = tpeTypeSymbol.name

    if (tpeTypeSymbol.flags.is(Flags.Module)) name = name.stripSuffix("$")

    var owner = tpeTypeSymbol.owner
    while (owner != defn.RootClass) {
      val ownerName = owner.name
      if (owner.flags.is(Flags.Package)) packages = ownerName :: packages
      else if (owner.flags.is(Flags.Module)) values = ownerName.stripSuffix("$") :: values
      else values = ownerName :: values
      owner = owner.owner
    }

    (new Owner(packages, values), name)
  }

  private def typeIdRepr(tpe0: TypeRepr, nested: List[TypeRepr]): TypeIdRepr = {
    val tpe = tpe0 match {
      // zio-prelude newtype representation in Scala 3 (same as Schema derivation logic)
      case TypeRef(compTpe, "Type") => compTpe
      case other                    => other
    }

    cache.getOrElseUpdate(
      tpe,
      if (tpe =:= TypeRepr.of[java.lang.String]) {
        new TypeIdRepr(Owner.fromNamespace(Namespace.scala), "String", Nil)
      } else {
        val isUnionTpe = isUnion(tpe)

        val (owner, baseName) =
          if (isUnionTpe) (Owner.root, "|")
          else ownerAndName(tpe)

        var name = baseName

        val tpeTypeArgs: List[TypeRepr] =
          if (isUnionTpe) allUnionTypes(tpe)
          else if (isNamedTuple(tpe)) {
            val tpeTypeArgs = typeArgs(tpe)
            val nTpe        = tpeTypeArgs.head
            val tTpe        = tpeTypeArgs.last
            val nTypeArgs   =
              if (isGenericTuple(nTpe)) genericTupleTypeArgs(nTpe)
              else typeArgs(nTpe)

            var comma  = false
            val labels = new java.lang.StringBuilder(name)
            labels.append('[')
            nTypeArgs.foreach { case ConstantType(StringConstant(str)) =>
              if (comma) labels.append(',')
              else comma = true
              labels.append(str)
            }
            labels.append(']')
            name = labels.toString

            if (isGenericTuple(tTpe)) genericTupleTypeArgs(tTpe)
            else typeArgs(tTpe)
          } else if (isGenericTuple(tpe)) genericTupleTypeArgs(tpe)
          else typeArgs(tpe)

        val params = tpeTypeArgs.map { arg =>
          val argId =
            if (nested.contains(arg)) typeIdRepr(anyTpe, Nil)
            else typeIdRepr(arg, arg :: nested)
          new TypeParam(argId)
        }

        new TypeIdRepr(owner, name, params)
      }
    )
  }

  private def toExpr(repr: TypeIdRepr): Expr[TypeIdRepr] = {
    def ownerExpr(owner: Owner): Expr[Owner] =
      '{ new Owner(${ Expr(owner.packages) }, ${ Expr(owner.values) }) }

    def paramExpr(p: TypeParam): Expr[TypeParam] =
      '{ new TypeParam(${ toExpr(p.id) }) }

    val ps: Expr[List[TypeParam]] = Expr.ofList(repr.params.map(paramExpr))
    '{ new TypeIdRepr(${ ownerExpr(repr.owner) }, ${ Expr(repr.name) }, $ps) }
  }

  def derive[A: Type]: Expr[TypeId.OfType] = {
    val repr = typeIdRepr(TypeRepr.of[A].dealias, Nil)
    '{ ${ toExpr(repr) }.asInstanceOf[TypeId.OfType] }
  }
}
