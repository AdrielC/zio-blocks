package zio.blocks.schema

/**
 * Runtime owner information for a type identifier.
 *
 * This is intentionally "boring": it captures the package path and the chain of
 * enclosing values / types (e.g. objects) as strings.
 */
final case class Owner(packages: List[String], values: List[String] = Nil) {
  lazy val elements: List[String] = packages ::: values
}

object Owner {
  val root: Owner = new Owner(Nil, Nil)

  private[schema] def fromNamespace(namespace: Namespace): Owner =
    new Owner(namespace.packages.toList, namespace.values.toList)

  private[schema] def toNamespace(owner: Owner): Namespace =
    new Namespace(owner.packages, owner.values)
}
