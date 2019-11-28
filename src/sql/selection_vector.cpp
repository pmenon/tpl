#include "sql/selection_vector.h"

#include "util/vector_util.h"

namespace tpl::sql {

void SelectionVector::BuildFromBitVector(const uint64_t *bits, uint32_t num_bits) {
  size_ = util::VectorUtil::BitVectorToSelectionVector(bits, num_bits, sel_vector_);
}

std::string SelectionVector::ToString() const {
  std::string result = "SelVec(size=" + std::to_string(size_) + ")=[";
  bool first = true;
  ForEach([&](uint64_t index) {
    if (!first) result += ",";
    first = false;
    result += std::to_string(index);
  });
  return result;
}

void SelectionVector::Dump(std::ostream &os) const { os << ToString(); }

}  // namespace tpl::sql
