#include "sql/tuple_id_list.h"

#include <iostream>
#include <string>

#include "util/vector_util.h"

namespace tpl::sql {

uint32_t TupleIdList::AsSelectionVector(uint16_t *sel_vec) const {
  return util::VectorUtil::BitVectorToSelectionVector(bit_vector_.data_array(),
                                                      bit_vector_.num_bits(), sel_vec);
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
