#pragma once

#include "sql/vector.h"

namespace tpl::sql {

/**
 * A constant vector is a vector with a single, constant value in it.
 */
class ConstantVector : public Vector {
 public:
  explicit ConstantVector(const GenericValue &value) : Vector(value.type_id()), value_(value) {
    Reference(&value_);
  }

 private:
  GenericValue value_;
};

}  // namespace tpl::sql
