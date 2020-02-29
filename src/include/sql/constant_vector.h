#pragma once

#include <utility>

#include "sql/vector.h"

namespace tpl::sql {

/**
 * A constant vector is a vector with a single, constant value in it.
 */
class ConstantVector : public Vector {
 public:
  explicit ConstantVector(GenericValue value)
      : Vector(value.GetTypeId()), value_(std::move(value)) {
    Reference(&value_);
  }

 private:
  GenericValue value_;
};

}  // namespace tpl::sql
