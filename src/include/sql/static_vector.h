#pragma once

#include <array>

#include "common/common.h"
#include "sql/sql.h"
#include "sql/vector.h"

namespace tpl::sql {

/**
 * A static data whose type is known at compile-time and whose vector data is directly inlined into
 * the class's memory, thus avoiding an allocation. This is appropriate for stack-allocations.
 *
 * Upon construction, the capacity and size of the vector is equal to ::tpl::kDefaultVectorSize,
 * usually 2048 elements, and the count is 0. The vector must be resized before it can be used.
 *
 * @tparam T The primitive type of the vector.
 */
template <typename T>
class StaticVector : public Vector {
 public:
  /**
   * Create a new empty static vector. The capacity and size of the vector is determined by the
   * default vector size constant, usually 2048. Like Vector, the initial count is 0.
   */
  StaticVector() : Vector(tpl::sql::GetTypeId<T>()) {
    // Arrange for the vector to reference the inlined data.
    Reference(reinterpret_cast<byte *>(inlined_data_.data()), nullptr, 0);
  }

 private:
  // The underlying vector data.
  std::array<T, kDefaultVectorSize> inlined_data_;
};

}  // namespace tpl::sql
