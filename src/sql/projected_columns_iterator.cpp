#include "sql/projected_columns_iterator.h"
#include <iostream>
#include "sql/schema.h"
#include "storage/projected_columns.h"
#include "util/vector_util.h"

namespace tpl::sql {

ProjectedColumnsIterator::ProjectedColumnsIterator(
    const sql::Schema *sql_schema)
    : projected_column_(nullptr),
      sql_schema_(sql_schema),
      curr_idx_(0),
      num_selected_(0),
      selection_vector_{0},
      selection_vector_read_idx_(0),
      selection_vector_write_idx_(0) {
  selection_vector_[0] = ProjectedColumnsIterator::kInvalidPos;
}

ProjectedColumnsIterator::ProjectedColumnsIterator(
    storage::ProjectedColumns *projected_column, const sql::Schema *sql_schema)
    : ProjectedColumnsIterator(sql_schema) {
  SetProjectedColumn(projected_column);
}

void ProjectedColumnsIterator::SetProjectedColumn(
    storage::ProjectedColumns *projected_column) {
  projected_column_ = projected_column;
  num_selected_ = projected_column_->NumTuples();
  curr_idx_ = 0;
  selection_vector_[0] = kInvalidPos;
  selection_vector_read_idx_ = 0;
  selection_vector_write_idx_ = 0;
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op>
u32 ProjectedColumnsIterator::FilterColByValImpl(u32 col_idx, T val) {
  // Get the input column's data
  const T *input =
      reinterpret_cast<const T *>(projected_column_->ColumnStart(col_idx));

  // Use the existing selection vector if this VPI has been filtered
  const u32 *sel_vec = (IsFiltered() ? selection_vector_ : nullptr);

  if (IsFiltered()) std::cout << "ALREADY FILTERED" << std::endl;

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
u32 ProjectedColumnsIterator::FilterColByVal(u32 col_idx,
                                             const sql::Type &sql_type,
                                             FilterVal val) {
  switch (sql_type.type_id()) {
    case TypeId::SmallInt: {
      return FilterColByValImpl<i16, Op>(col_idx, val.si);
    }
    case TypeId::Integer: {
      return FilterColByValImpl<i32, Op>(col_idx, val.i);
    }
    case TypeId::BigInt: {
      return FilterColByValImpl<i64, Op>(col_idx, val.bi);
    }
    default: { throw std::runtime_error("Filter not supported on type"); }
  }
}

// clang-format off
template u32 ProjectedColumnsIterator::FilterColByVal<std::equal_to>(u32, const sql::Type&, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater>(u32, const sql::Type&, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::greater_equal>(u32, const sql::Type&, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less>(u32, const sql::Type&, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::less_equal>(u32, const sql::Type&, FilterVal);
template u32 ProjectedColumnsIterator::FilterColByVal<std::not_equal_to>(u32, const sql::Type&, FilterVal);
// clang-format on

}  // namespace tpl::sql
