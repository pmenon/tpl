#pragma once

#include <type_traits>

#include "sql/value.h"

namespace tpl::sql {

// Functor always returning true. Used as default predicate in evaluation wrappers.
struct True {
  template <typename... Args>
  constexpr bool operator()(Args &&...) const noexcept {
    return true;
  }
};

class UnaryFunction : public AllStatic {
 public:
  template <SQLValueType R, SQLValueType T, typename Op>
  static void EvalFast(R *result, const T &input, Op op) {
    const bool is_null = input.is_null;
    const auto val = op(input.val);
    *result = R(std::move(val), is_null);
  }

  template <SQLValueType R, SQLValueType T, typename Op, typename Pred = True>
  static void EvalHideNull(R *result, const T &a, Op op, Pred pred = {}) {
    // Return NULL if either input is NULL.
    if (a.is_null || !pred(a.val)) {
      *result = R::Null();
      return;
    }
    // All good, apply the function.
    const bool is_null = false;
    const auto val = op(a.val);
    *result = R(std::move(val), is_null);
  }
};

class BinaryFunction : public AllStatic {
 public:
  template <SQLValueType R, SQLValueType T1, SQLValueType T2, typename Op>
  static void EvalFast(R *result, const T1 &a, const T2 &b, Op op) {
    const bool is_null = a.is_null || b.is_null;
    const auto val = op(a.val, b.val);
    *result = R(std::move(val), is_null);
  }

  template <SQLValueType R, SQLValueType T1, SQLValueType T2, typename Op, typename Pred = True>
  static void EvalHideNull(R *result, const T1 &a, const T2 &b, Op op, Pred pred = {}) {
    // Return NULL if either input is NULL.
    if (a.is_null || b.is_null || !pred(a.val, b.val)) {
      *result = R::Null();
      return;
    }
    // All good, apply the function.
    const bool is_null = false;
    const auto val = op(a.val, b.val);
    *result = R(std::move(val), is_null);
  }
};

}  // namespace tpl::sql
