#include "sql/tuple_id_list.h"

#include <iostream>
#include <string>

#include "util/vector_util.h"

namespace tpl::sql {

void TupleIdList::BuildFromSelectionVector(sel_t *sel_vector, uint32_t size) {
  for (uint32_t i = 0; i < size; i++) {
    bit_vector_.Set(sel_vector[i]);
  }
}

uint32_t TupleIdList::AsSelectionVector(sel_t *sel_vec) const {
  uint32_t k = 0;
  bit_vector_.IterateSetBits([&](const uint32_t i) { sel_vec[k++] = i; });
  return k;
}

std::string TupleIdList::ToString() const {
  std::string result = "TIDs=[";
  bool first = true;
  Iterate([&](const uint64_t i) {
    if (!first) result += ",";
    first = false;
    result += std::to_string(i);
  });
  result += "]";
  return result;
}

void TupleIdList::Dump(std::ostream &stream) const { stream << ToString() << std::endl; }

}  // namespace tpl::sql
