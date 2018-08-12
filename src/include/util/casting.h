#pragma once

#include <type_traits>

#include "util/macros.h"

namespace tpl::util {

namespace internal {

// Ask the target class whether the source is actually a proper instance
template <typename To, typename From, typename Enabled = void>
struct IsHelper {
  static bool Impl(const From &x) { return To::classof(&x); }
};

// Always allow upcasts
template <typename To, typename From>
struct IsHelper<To, From, std::enable_if_t<std::is_base_of_v<To, From>>> {
  static bool Impl(const From &x) { return true; }
};

template <typename To, typename From>
struct IsHelperConstStrip {
  static bool Impl(const From &x) { return IsHelper<To, From>::Impl(x); }
};

template <typename To, typename From>
struct IsHelperConstStrip<To, From *> {
  static bool Impl(const From *x) {
    TPL_ASSERT(x != nullptr && "Is() can't be called on a NULL pointer");
    return IsHelper<To, From>::Impl(*x);
  }
};

template <typename To, typename From>
struct IsHelperConstStrip<To, const From *> {
  static bool Impl(const From *x) {
    TPL_ASSERT(x != nullptr && "Is() can't be called on a NULL pointer");
    return IsHelper<To, From>::Impl(*x);
  }
};

template <typename To, typename From>
struct IsHelperConstStrip<To, const From *const> {
  static bool Impl(const From *x) {
    TPL_ASSERT(x != nullptr && "Is() can't be called on a NULL pointer");
    return IsHelper<To, From>::Impl(*x);
  }
};

}  // namespace internal

template <typename To, typename From>
bool Is(const From &x) {
  return internal::IsHelperConstStrip<To, From>::Impl(x);
}

}  // namespace tpl::util
