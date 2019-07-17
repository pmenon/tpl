#include "sql/vector_projection.h"

#include <iostream>
#include <memory>
#include <vector>

#include "sql/column_vector_iterator.h"
#include "sql/vector.h"
#include "util/bit_util.h"

namespace tpl::sql {

VectorProjection::VectorProjection() : tuple_count_(0) {}

VectorProjection::VectorProjection(
    const std::vector<const Schema::ColumnInfo *> &col_infos)
    : tuple_count_(0) {
  InitializeEmpty(col_infos);
}

void VectorProjection::Initialize(
    const std::vector<const Schema::ColumnInfo *> &col_infos) {
  tuple_count_ = 0;
  column_info_ = col_infos;
  columns_.resize(col_infos.size());
  for (u32 i = 0; i < columns_.size(); i++) {
    const TypeId col_type = col_infos[i]->sql_type.GetPrimitiveTypeId();
    columns_[i] = std::make_unique<Vector>(col_type, true, true);
  }
}

void VectorProjection::InitializeEmpty(
    const std::vector<const Schema::ColumnInfo *> &col_infos) {
  tuple_count_ = 0;
  column_info_ = col_infos;
  columns_.resize(col_infos.size());
  for (u32 i = 0; i < columns_.size(); i++) {
    const TypeId col_type = col_infos[i]->sql_type.GetPrimitiveTypeId();
    columns_[i] = std::make_unique<Vector>(col_type);
  }
}

void VectorProjection::ResetColumn(byte *col_data, u32 *col_null_bitmap,
                                   u32 col_idx, u32 num_tuples) {
  // Reset tuple count
  tuple_count_ = num_tuples;

  // Reset the vector to reference the input data
  auto col_type_id = GetColumnInfo(col_idx)->sql_type.GetPrimitiveTypeId();
  columns_[col_idx]->Reference(col_type_id, col_data, col_null_bitmap,
                               num_tuples);
}

void VectorProjection::ResetColumn(
    const std::vector<ColumnVectorIterator> &col_iters, const u32 col_idx) {
  ResetColumn(col_iters[col_idx].col_data(),
              col_iters[col_idx].col_null_bitmap(), col_idx,
              col_iters[col_idx].NumTuples());
}

std::string VectorProjection::ToString() const {
  auto result =
      "VectorProjection(#cols=" + std::to_string(columns_.size()) + "):\n";
  for (auto &col : columns_) {
    result += "- " + col->ToString() + "\n";
  }
  return result;
}

void VectorProjection::Dump(std::ostream &stream) const {
  stream << ToString() << std::endl;
}

}  // namespace tpl::sql
