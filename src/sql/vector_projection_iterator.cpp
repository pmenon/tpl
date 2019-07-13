#include "sql/vector_projection_iterator.h"

#include "util/vector_util.h"

namespace tpl::sql {

VectorProjectionIterator::VectorProjectionIterator()
    : vector_projection_(nullptr),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = VectorProjectionIterator::kInvalidPos;
}

VectorProjectionIterator::VectorProjectionIterator(VectorProjection *vp)
    : VectorProjectionIterator() {
  SetVectorProjection(vp);
}

void VectorProjectionIterator::SetVectorProjection(VectorProjection *vp) {
  vector_projection_ = vp;
  num_selected_ = vp->GetTupleCount();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByValImpl(u32 col_idx, T val) {
  // Get the input column's data
  const Vector *const column = vector_projection_->GetColumn(col_idx);
  const T *input = reinterpret_cast<const T *>(column->data());

  // Use the existing selection vector if this VPI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ = util::VectorUtil::FilterVectorByVal<T, Op>(
      input, num_selected_, val, selection_vector_, sel_vec);

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

// Filter an entire column's data by the provided constant value
template <template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByVal(const u32 col_idx,
                                             const FilterVal val) {
  auto *col_type = vector_projection_->GetColumnInfo(col_idx);

  switch (col_type->sql_type.id()) {
    case SqlTypeId::SmallInt: {
      return FilterColByValImpl<i16, Op>(col_idx, val.si);
    }
    case SqlTypeId::Integer: {
      return FilterColByValImpl<i32, Op>(col_idx, val.i);
    }
    case SqlTypeId::BigInt: {
      return FilterColByValImpl<i64, Op>(col_idx, val.bi);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

template <typename T, template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByColImpl(const u32 col_idx_1,
                                                 const u32 col_idx_2) {
  // Get the input column's data
  const Vector *const column_1 = vector_projection_->GetColumn(col_idx_1);
  const Vector *const column_2 = vector_projection_->GetColumn(col_idx_2);
  const T *input_1 = reinterpret_cast<const T *>(column_1->data());
  const T *input_2 = reinterpret_cast<const T *>(column_2->data());

  // Use the existing selection vector if this VPI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  // Filter!
  selection_vector_write_idx_ = util::VectorUtil::FilterVectorByVector<T, Op>(
      input_1, input_2, num_selected_, selection_vector_, sel_vec);

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

template <template <typename> typename Op>
u32 VectorProjectionIterator::FilterColByCol(const u32 col_idx_1,
                                             const u32 col_idx_2) {
  auto *col_1_info = vector_projection_->GetColumnInfo(col_idx_1);
  UNUSED auto *col_2_info = vector_projection_->GetColumnInfo(col_idx_2);
  TPL_ASSERT(col_1_info->sql_type.Equals(col_2_info->sql_type),
             "Incompatible column types for filter");

  switch (col_1_info->sql_type.id()) {
    case SqlTypeId::SmallInt: {
      return FilterColByColImpl<i16, Op>(col_idx_1, col_idx_2);
    }
    case SqlTypeId::Integer: {
      return FilterColByColImpl<i32, Op>(col_idx_1, col_idx_2);
    }
    case SqlTypeId::BigInt: {
      return FilterColByColImpl<i64, Op>(col_idx_1, col_idx_2);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

// ---------------------------------------------------------
// Template instantiations
// ---------------------------------------------------------

// clang-format off
template u32 VectorProjectionIterator::FilterColByVal<std::equal_to>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::greater>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::greater_equal>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::less>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::less_equal>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByVal<std::not_equal_to>(u32, FilterVal);
template u32 VectorProjectionIterator::FilterColByCol<std::equal_to>(u32, u32);
template u32 VectorProjectionIterator::FilterColByCol<std::greater>(u32, u32);
template u32 VectorProjectionIterator::FilterColByCol<std::greater_equal>(u32, u32);
template u32 VectorProjectionIterator::FilterColByCol<std::less>(u32, u32);
template u32 VectorProjectionIterator::FilterColByCol<std::less_equal>(u32, u32);
template u32 VectorProjectionIterator::FilterColByCol<std::not_equal_to>(u32, u32);
// clang-format on

}  // namespace tpl::sql
