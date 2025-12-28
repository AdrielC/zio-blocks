package zio.blocks.schema

/**
 * The single runtime representation of a type identifier.
 *
 * Everything else in the TypeId system is phantom typing / tagging.
 */
final case class TypeIdRepr(
  owner: Owner,
  name: String,
  params: List[TypeParam]
)
