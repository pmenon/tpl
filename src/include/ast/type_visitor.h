#pragma once

#include "ast/type.h"

namespace tpl::ast {

/**
 * Generic visitor
 */
template <typename Subclass>
class TypeVisitor {
 public:
  void Visit(Type *type) { return impl().Visit(type); }

 protected:
  Subclass &impl() { return *static_cast<Subclass *>(this); }
};

#define GEN_VISIT_CASE(kind)                                                \
  case ::tpl::ast::Type::Kind::kind: {                                      \
    return this->impl().Visit##kind(static_cast<::tpl::ast::kind *>(type)); \
  }

#define DEFINE_AST_VISITOR_METHOD()                     \
  void Visit(::tpl::ast::Type *type) {                  \
    switch (type->kind()) { TYPE_LIST(GEN_VISIT_CASE) } \
  }

}  // namespace tpl::ast