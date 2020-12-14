#pragma once

#include "ast/type.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/fwd.h"

namespace tpl::sql::codegen::edsl {

/**
 * Trait to build the AST for a type. The type should be an EDSL value type.
 * @tparam T The introspected type.
 */
template <typename T>
struct TypeBuilder;

/**
 * Const types don't have counter parts in TPL. Forward to the non-const implementation.
 * @tparam T The const type.
 */
template <typename T>
struct TypeBuilder<const T> : public TypeBuilder<T> {};

/**
 * Volatile types don't have counter parts in TPL. Forward to the non-volatile implementation.
 * @tparam T The volatile type.
 */
template <typename T>
struct TypeBuilder<volatile T> : public TypeBuilder<T> {};

/**
 * Const-volatile types don't have counter parts in TPL. Forward to the normal implementation.
 * @tparam T The const-volatile type.
 */
template <typename T>
struct TypeBuilder<const volatile T> : public TypeBuilder<T> {};

#define GEN_TYPE_BUILDER(Type, ...)                              \
  /**                                                            \
   * A type builder for all primitive/builtin types.             \
   */                                                            \
  template <>                                                    \
  struct TypeBuilder<Type> {                                     \
    /**                                                          \
     * @return The AST representation of this primitive type.    \
     */                                                          \
    static ast::Expression *MakeTypeRepr(CodeGen *codegen) {     \
      return codegen->BuiltinType(ast::BuiltinType::Kind::Type); \
    }                                                            \
  };

BUILTIN_TYPE_LIST(GEN_TYPE_BUILDER, GEN_TYPE_BUILDER, GEN_TYPE_BUILDER)

#undef GEN_TYPE_BUILDER

/**
 * Trait for pointers.
 * @tparam T The pointee type.
 */
template <typename T>
struct TypeBuilder<Ptr<T>> {
  /**
   * @return The AST type representation of this pointer type.
   */
  static ast::Expression *MakeTypeRepr(CodeGen *codegen) {
    return codegen->PointerType(TypeBuilder<T>::MakeTypeRepr(codegen));
  }
};

/**
 * Arrays with unknown length.
 */
template <std::size_t N, typename T>
struct TypeBuilder<Array<N, T>> {
  /**
   * @return The AST type representation of this unbounded array.
   */
  static ast::Expression *MakeTypeRepr(CodeGen *codegen) {
    return codegen->ArrayType(N, TypeBuilder<T>::MakeTypeRepr(codegen));
  }
};

}  // namespace tpl::sql::codegen::edsl
