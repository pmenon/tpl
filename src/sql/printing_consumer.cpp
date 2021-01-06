#include "sql/printing_consumer.h"

#include <iomanip>

#include "sql/planner/plannodes/output_schema.h"
#include "sql/sql.h"
#include "sql/value.h"

namespace tpl::sql {

PrintingConsumer::PrintingConsumer(std::ostream &os,
                                   const sql::planner::OutputSchema *output_schema)
    : os_(os), output_schema_(output_schema) {}

void PrintingConsumer::PrintTuple(const byte *const tuple) const {
  bool first = true;
  const auto &cols = output_schema_->GetColumns();
  const auto &col_offsets = output_schema_->GetColumnOffsets();
  for (std::size_t i = 0; i < cols.size(); i++) {
    // Comma.
    if (!first) os_ << ",";
    first = false;

    const byte *const col_ptr = tuple + col_offsets[i];

    // Column value
    switch (cols[i].GetType().GetTypeId()) {
      case SqlTypeId::Boolean: {
        const auto val = reinterpret_cast<const BoolVal *>(col_ptr);
        os_ << (val->is_null ? "NULL" : val->val ? "True" : "False");
        break;
      }
      case SqlTypeId::TinyInt:
      case SqlTypeId::SmallInt:
      case SqlTypeId::Integer:
      case SqlTypeId::BigInt: {
        const auto val = reinterpret_cast<const Integer *>(col_ptr);
        os_ << (val->is_null ? "NULL" : std::to_string(val->val));
        break;
      }
      case SqlTypeId::Real:
      case SqlTypeId::Double:
      case SqlTypeId::Decimal: {
        const auto val = reinterpret_cast<const Real *>(col_ptr);
        if (val->is_null) {
          os_ << "NULL";
        } else {
          os_ << std::fixed << std::setprecision(2) << val->val << std::dec;
        }
        break;
      }
      case SqlTypeId::Date: {
        const auto val = reinterpret_cast<const DateVal *>(col_ptr);
        os_ << (val->is_null ? "NULL" : val->val.ToString());
        break;
      }
      case SqlTypeId::Timestamp: {
        const auto val = reinterpret_cast<const TimestampVal *>(col_ptr);
        os_ << (val->is_null ? "NULL" : val->val.ToString());
        break;
      }
      case SqlTypeId::Char:
      case SqlTypeId::Varchar: {
        const auto val = reinterpret_cast<const StringVal *>(col_ptr);
        if (val->is_null) {
          os_ << "NULL";
        } else {
          os_ << "'" << val->val.GetStringView() << "'";
        }
        break;
      }
    }
  }

  // New line
  os_ << std::endl;
}

void PrintingConsumer::Consume(const OutputBuffer &batch) {
  for (const byte *raw_tuple : batch) {
    PrintTuple(raw_tuple);
  }
}

}  // namespace tpl::sql
