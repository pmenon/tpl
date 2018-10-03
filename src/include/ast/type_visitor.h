#pragma once

#include "ast/type.h"

namespace tpl::ast {

/**
 * Generic visitor for type hierarchies
 */
template <typename Impl, typename RetType = void>
class TypeVisitor {
 public:
#define DISPATCH(Type)                           \
  return static_cast<Impl *>(this)->Visit##Type( \
      static_cast<const Type *>(type));

  RetType Visit(const Type *type) {
    switch (type->kind()) {
      default: { llvm_unreachable("Impossible node type"); }
#define T(kind)          \
  case Type::Kind::kind: \
    DISPATCH(kind)
        TYPE_LIST(T)
#undef T
    }
  }

  RetType VisitType(UNUSED const Type *type) { return RetType(); }

#define T(Type) \
  RetType Visit##Type(const Type *type) { DISPATCH(Type); }
  TYPE_LIST(T)
#undef T

#undef DISPATCH
};

}  // namespace tpl::ast