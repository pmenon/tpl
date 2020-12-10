#pragma once

#include <type_traits>

#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/codegen.h"
#include "sql/codegen/edsl/fwd.h"
#include "sql/codegen/edsl/traits.h"

namespace tpl::sql::codegen::edsl {

/**
 * Literal specialization for C++ booleans.
 */
template <>
class Literal<Bool, void> {
 public:
  /**
   * Create a boolean literal with the given value.
   * @param value The value of the boolean.
   */
  Literal(CodeGen *codegen, bool value) : codegen_(codegen), value_(value) {}

  /**
   * Evaluate the boolean literal as a TPL expression.
   * @param codegen The code generator.
   * @return The value of the boolean literal as an AST node.
   */
  ast::Expression *Eval() const { return codegen_->ConstBool(value_); }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The raw boolean value.
  bool value_;
};

/**
 * Literal specialization for integral types.
 * @tparam T The C++ integral type.
 */
template <typename T>
class Literal<T, std::enable_if_t<trait_details::IsInteger<T>::value>> {
 public:
  using CppType = typename Traits<T>::CppType;

  Literal(CodeGen *codegen, CppType value) : codegen_(codegen), value_(value) {}

  ast::Expression *Eval() const {
    if constexpr (sizeof(value_) == 1) {
      if constexpr (std::is_signed_v<T>) {
        return codegen_->Const8(value_);
      } else {
        return codegen_->Const8(value_);
      }
    } else if constexpr (sizeof(value_) == 2) {
      if constexpr (std::is_signed_v<T>) {
        return codegen_->Const16(value_);
      } else {
        return codegen_->Const16(value_);
      }
    } else if constexpr (sizeof(value_) == 4) {
      if constexpr (std::is_signed_v<T>) {
        return codegen_->Const32(value_);
      } else {
        return codegen_->Const32(value_);
      }
    } else {
      if constexpr (std::is_signed_v<T>) {
        return codegen_->Const64(value_);
      } else {
        return codegen_->Const64(value_);
      }
    }
  }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The raw C++ value.
  CppType value_;
};

/**
 * Literal specialization for 32- or 64-bit floating-point types.
 * @tparam T The C++ floating point type.
 */
template <typename T>
class Literal<T, std::enable_if_t<trait_details::IsFloatingPoint<T>::value>> {
 public:
  using CppType = typename Traits<T>::CppType;

  Literal(CodeGen *codegen, CppType value) : codegen_(codegen), value_(value) {}

  ast::Expression *Eval() const { return codegen_->ConstDouble(value_); }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const noexcept { return codegen_; }

 private:
  // The code generator instance.
  CodeGen *codegen_;
  // The raw C++ value.
  CppType value_;
};

/**
 * Trait specialization for literals.
 * @tparam T The ETL type.
 */
template <typename T>
struct Traits<Literal<T>> {
  /** The value type of the expression. */
  using ValueType = T;
  /** Literals are ETL values. */
  static constexpr bool kIsETL = true;
};

}  // namespace tpl::sql::codegen::edsl