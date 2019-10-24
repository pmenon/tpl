#pragma once

#include "ast/type.h"

namespace tpl::ast {

/**
 * Generic visitor for type hierarchies.
 */
template <typename Subclass, typename RetType = void>
class TypeVisitor {
 public:
#define DISPATCH(Type) return this->Impl()->Visit##Type(static_cast<const Type *>(type));

  /**
   * Begin type traversal at the given type node.
   * @param type The type to begin traversal at.
   * @return Template-specific return type.
   */
  RetType Visit(const Type *type) {
#define T(TypeClass)            \
  case Type::TypeId::TypeClass: \
    DISPATCH(TypeClass)

    switch (type->type_id()) {
      TYPE_LIST(T)
      default: { UNREACHABLE("Impossible node type"); }

#undef T
    }
  }

  /**
   * No-op base implementation for all type nodes.
   * @param type The type to visit.
   * @return No-arg constructed return.
   */
  RetType VisitType(UNUSED const Type *type) { return RetType(); }

#define T(Type) \
  RetType Visit##Type(const Type *type) { DISPATCH(Type); }
  TYPE_LIST(T)
#undef T

#undef DISPATCH

 protected:
  Subclass *Impl() { return static_cast<Subclass *>(this); }
};

}  // namespace tpl::ast
