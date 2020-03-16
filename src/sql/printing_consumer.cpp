#include "sql/printing_consumer.h"

#include <iomanip>

#include "sql/planner/plannodes/output_schema.h"
#include "sql/sql.h"
#include "sql/value.h"

namespace tpl::sql {

PrintingConsumer::PrintingConsumer(std::ostream &os,
                                   const sql::planner::OutputSchema *output_schema)
    : os_(os), output_schema_(output_schema) {}

void PrintingConsumer::PrintTuple(const byte *tuple) const {
  bool first = true;
  for (const auto &col : output_schema_->GetColumns()) {
    // Comma
    if (!first) os_ << ",";
    first = false;

    // Column value
    switch (GetSqlTypeFromInternalType(col.GetType())) {
      case SqlTypeId::Boolean: {
        const auto val = reinterpret_cast<const BoolVal *>(tuple);
        os_ << (val->is_null ? "NULL" : val->val ? "True" : "False");
        tuple += sizeof(BoolVal);
        break;
      }
      case SqlTypeId::TinyInt:
      case SqlTypeId::SmallInt:
      case SqlTypeId::Integer:
      case SqlTypeId::BigInt: {
        const auto val = reinterpret_cast<const Integer *>(tuple);
        os_ << (val->is_null ? "NULL" : std::to_string(val->val));
        tuple += sizeof(Integer);
        break;
      }
      case SqlTypeId::Real:
      case SqlTypeId::Double:
      case SqlTypeId::Decimal: {
        const auto val = reinterpret_cast<const Real *>(tuple);
        if (val->is_null) {
          os_ << "NULL";
        } else {
          os_ << std::fixed << std::setprecision(2) << val->val << std::dec;
        }
        tuple += sizeof(Real);
        break;
      }
      case SqlTypeId::Date: {
        const auto val = reinterpret_cast<const DateVal *>(tuple);
        os_ << (val->is_null ? "NULL" : val->val.ToString());
        tuple += sizeof(DateVal);
        break;
      }
      case SqlTypeId::Timestamp: {
        const auto val = reinterpret_cast<const TimestampVal *>(tuple);
        os_ << (val->is_null ? "NULL" : val->val.ToString());
        tuple += sizeof(TimestampVal);
        break;
      }
      case SqlTypeId::Char:
      case SqlTypeId::Varchar: {
        const auto val = reinterpret_cast<const StringVal *>(tuple);
        if (val->is_null) {
          os_ << "NULL";
        } else {
          os_ << "'" << val->val.GetStringView() << "'";
        }
        tuple += sizeof(StringVal);
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
