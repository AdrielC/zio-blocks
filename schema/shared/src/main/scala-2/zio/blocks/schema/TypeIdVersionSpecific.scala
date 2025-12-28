package zio.blocks.schema

import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import scala.reflect.NameTransformer

trait TypeIdVersionSpecific {
  def derive[A]: TypeId.OfType = macro TypeIdVersionSpecific.derive[A]
}

private object TypeIdVersionSpecific {
  def derive[A: c.WeakTypeTag](c: blackbox.Context): c.Expr[TypeId.OfType] = {
    import c.universe._
    import c.internal._

    def typeArgs(tpe: Type): List[Type] = CommonMacroOps.typeArgs(c)(tpe)

    def companion(tpe: Type): Symbol = {
      val comp = tpe.typeSymbol.companion
      if (comp.isModule) comp
      else {
        val ownerChainOf = (s: Symbol) => Iterator.iterate(s)(_.owner).takeWhile(_ != NoSymbol).toArray.reverseIterator
        val path         = ownerChainOf(tpe.typeSymbol)
          .zipAll(ownerChainOf(enclosingOwner), NoSymbol, NoSymbol)
          .dropWhile(x => x._1 == x._2)
          .takeWhile(x => x._1 != NoSymbol)
          .map(x => x._1.name.toTermName)
        if (path.isEmpty) NoSymbol
        else c.typecheck(path.foldLeft[Tree](Ident(path.next()))(Select(_, _)), silent = true).symbol
      }
    }

    val anyTpe = definitions.AnyTpe

    val cache = new mutable.HashMap[Type, TypeIdRepr]

    def ownerAndName(tpe: Type): (Owner, String) = {
      var packages = List.empty[String]
      var values   = List.empty[String]
      val tpeSym   = tpe.typeSymbol
      var name     = NameTransformer.decode(tpeSym.name.toString)

      val comp  = companion(tpe)
      var owner =
        if (comp == null) tpeSym
        else if (comp == NoSymbol) {
          name += ".type"
          tpeSym.asClass.module
        } else comp

      while ({
        owner = owner.owner
        owner.owner != NoSymbol
      }) {
        val ownerName = NameTransformer.decode(owner.name.toString)
        if (owner.isPackage || owner.isPackageClass) packages = ownerName :: packages
        else values = ownerName :: values
      }

      (new Owner(packages, values), name)
    }

    def typeIdRepr(tpe0: Type, nested: List[Type]): TypeIdRepr = {
      val tpe = tpe0 match {
        case TypeRef(compTpe, typeSym, Nil) if typeSym.name.toString == "Type" => compTpe
        case other                                                             => other
      }

      cache.getOrElseUpdate(
        tpe, {
          val (owner, name) =
            if (tpe =:= typeOf[java.lang.String]) (Owner.fromNamespace(Namespace.scala), "String")
            else ownerAndName(tpe)

          val params = typeArgs(tpe).map { arg =>
            val argId =
              if (nested.contains(arg)) typeIdRepr(anyTpe, Nil)
              else typeIdRepr(arg, arg :: nested)
            new TypeParam(argId)
          }

          new TypeIdRepr(owner, name, params)
        }
      )
    }

    def toTree(repr: TypeIdRepr): Tree = {
      def ownerTree(owner: Owner): Tree =
        q"new _root_.zio.blocks.schema.Owner(${owner.packages}, ${owner.values})"

      def paramTree(p: TypeParam): Tree =
        q"new _root_.zio.blocks.schema.TypeParam(${toTree(p.id)})"

      q"new _root_.zio.blocks.schema.TypeIdRepr(${ownerTree(repr.owner)}, ${repr.name}, _root_.scala.List(..${repr.params.map(paramTree)}))"
    }

    val reprTree = toTree(typeIdRepr(weakTypeOf[A].dealias, Nil))

    c.Expr[TypeId.OfType](
      q"_root_.zio.blocks.schema.TypeId.unsafeTag[_root_.zio.blocks.schema.TypeId.KindTag.Type]($reprTree)"
    )
  }
}

