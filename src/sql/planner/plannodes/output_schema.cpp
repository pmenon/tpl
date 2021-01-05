#include "sql/planner/plannodes/output_schema.h"

#include "sql/value.h"

namespace tpl::sql::planner {

namespace {

std::pair<std::size_t, std::size_t> GetTypeSizeAndAlignment(SqlTypeId type) {
  switch (type) {
    case SqlTypeId::Boolean:
      return {sizeof(BoolVal), alignof(BoolVal)};
    case SqlTypeId::TinyInt:
    case SqlTypeId::SmallInt:
    case SqlTypeId::Integer:
    case SqlTypeId::BigInt:
      return {sizeof(Integer), alignof(Integer)};
    case SqlTypeId::Real:
    case SqlTypeId::Double:
    case SqlTypeId::Decimal:
      return {sizeof(Real), alignof(Real)};
    case SqlTypeId::Date:
      return {sizeof(DateVal), alignof(DateVal)};
    case SqlTypeId::Timestamp:
      return {sizeof(TimestampVal), alignof(TimestampVal)};
    case SqlTypeId::Char:
    case SqlTypeId::Varchar:
      return {sizeof(StringVal), alignof(StringVal)};
    default:
      UNREACHABLE("Impossible type.");
  }
}

}  // namespace

OutputSchema::OutputSchema(std::vector<Column> columns) : columns_(std::move(columns)) {
  // Reserve now.
  column_offsets_.reserve(columns_.size());
  // Running offset.
  std::size_t offset = 0;
  // Compute offset for each column.
  for (std::size_t i = 0; i < columns_.size(); i++) {
    const auto sql_type_id = GetSqlTypeFromInternalType(columns_[i].GetType());
    const auto [size, align] = GetTypeSizeAndAlignment(sql_type_id);
    if (i != 0) {
      offset = util::MathUtil::AlignTo(offset, align);
    }
    column_offsets_.emplace_back(offset);
    offset += size;
  }
}

OutputSchema::Column OutputSchema::GetColumn(const uint32_t col_idx) const {
  TPL_ASSERT(col_idx < columns_.size(), "column id is out of bounds for this Schema");
  return columns_[col_idx];
}

const std::vector<OutputSchema::Column> &OutputSchema::GetColumns() const { return columns_; }

const std::vector<std::size_t> &OutputSchema::GetColumnOffsets() const { return column_offsets_; }

std::size_t OutputSchema::ComputeOutputRowSize() const {
  const auto sql_type_id = GetSqlTypeFromInternalType(columns_.back().GetType());
  const auto [size, align] = GetTypeSizeAndAlignment(sql_type_id);
  return column_offsets_.back() + size;
}

uint32_t OutputSchema::NumColumns() const { return columns_.size(); }

std::string OutputSchema::ToString() const {
  std::string result = "Schema(" + std::to_string(NumColumns()) + ")=[";
  bool first = true;
  for (const auto &col : columns_) {
    if (!first) result += ",";
    first = false;
    result += TypeIdToString(col.GetType()) + (col.GetNullable() ? "(NULLABLE)" : "");
  }
  result += "]";
  return result;
}

}  // namespace tpl::sql::planner
