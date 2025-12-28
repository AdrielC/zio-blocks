package zio.blocks.schema

/**
 * A type parameter inside a TypeId.
 *
 * This is a wrapper ADT to keep the runtime representation extensible (aliases,
 * opaques, structural types, etc.) without changing TypeIdRepr.
 */
final case class TypeParam(id: TypeIdRepr)
