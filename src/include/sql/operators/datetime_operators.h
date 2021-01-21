#pragma once

#include "sql/runtime_types.h"

namespace tpl::sql {

/**
 * Functor implementing the EXTRACT(CENTURY from date) operator.
 */
struct Century {
  int64_t operator()(const Timestamp &input) const noexcept {
    const auto year = input.ExtractYear();
    if (year > 0) {
      return (year + 99) / 100;
    } else {
      return -((99 - (year - 1)) / 100);
    }
  }
};

/**
 * Functor implementing the EXTRACT(DECADE from date) operator.
 */
struct Decade {
  int64_t operator()(const Timestamp &input) const noexcept {
    const auto year = input.ExtractYear();
    if (year >= 0) {
      return year / 10;
    } else {
      return -((8 - (year - 1)) / 10);
    }
  }
};

/**
 * Functor implementing the EXTRACT(Quarter from date) operator.
 */
struct Quarter {
  int64_t operator()(const Timestamp &input) const noexcept {
    const auto month = input.ExtractMonth();
    return (month - 1) / 3 + 1;
  }
};

}  // namespace tpl::sql
