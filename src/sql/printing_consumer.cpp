#include "sql/printing_consumer.h"

#include <iomanip>

#include "sql/schema.h"
#include "sql/sql.h"
#include "sql/value.h"

namespace tpl::sql {

// TODO(pmenon): Templatize all "Print" functions based on NULL-ability if they become a bottleneck
// TODO(pmenon): Compressed values?

namespace {

const byte *PrintBoolVal(std::ostream &os, const byte *ptr) {
  auto *val = reinterpret_cast<const BoolVal *>(ptr);
  os << (val->is_null ? "NULL" : val->val ? "True" : "False");
  return ptr + sizeof(BoolVal);
}

const byte *PrintIntegerVal(std::ostream &os, const byte *ptr) {
  auto *val = reinterpret_cast<const Integer *>(ptr);
  os << (val->is_null ? "NULL" : std::to_string(val->val));
  return ptr + sizeof(Integer);
}

const byte *PrintRealVal(std::ostream &os, const byte *ptr) {
  auto *val = reinterpret_cast<const Real *>(ptr);
  if (val->is_null) {
    os << "NULL";
  } else {
    os << std::fixed << std::setprecision(2) << val->val << std::dec;
  }
  return ptr + sizeof(Real);
}

const byte *PrintDateVal(std::ostream &os, const byte *ptr) {
  auto *val = reinterpret_cast<const DateVal *>(ptr);
  os << (val->is_null ? "NULL" : val->val.ToString());
  return ptr + sizeof(DateVal);
}

const byte *PrintStringVal(std::ostream &os, const byte *ptr) {
  auto *val = reinterpret_cast<const StringVal *>(ptr);
  if (val->is_null) {
    os << "NULL";
  } else {
    os << "'" << val->val.GetStringView() << "'";
  }
  return ptr + sizeof(StringVal);
}

}  // namespace

PrintingConsumer::PrintingConsumer(std::ostream &os, const sql::Schema &output_schema)
    : os_(os), output_schema_(output_schema) {
  col_printers_.reserve(output_schema_.GetColumnCount());
  for (const auto &col_info : output_schema_.GetColumns()) {
    switch (col_info.sql_type.id()) {
      case SqlTypeId::Boolean:
        col_printers_.emplace_back(PrintBoolVal);
        break;
      case SqlTypeId::TinyInt:
      case SqlTypeId::SmallInt:
      case SqlTypeId::Integer:
      case SqlTypeId::BigInt: {
        col_printers_.emplace_back(PrintIntegerVal);
        break;
      }
      case SqlTypeId::Real:
      case SqlTypeId::Double:
      case SqlTypeId::Decimal: {
        col_printers_.emplace_back(PrintRealVal);
        break;
      }
      case SqlTypeId::Date: {
        col_printers_.emplace_back(PrintDateVal);
        break;
      }
      case SqlTypeId::Char:
      case SqlTypeId::Varchar: {
        col_printers_.emplace_back(PrintStringVal);
        break;
      }
    }
  }
}

void PrintingConsumer::Consume(const OutputBuffer &batch) {
  for (const auto raw_tuple : batch) {
    const byte *ptr = raw_tuple;
    bool first = true;
    for (const auto &col_printer : col_printers_) {
      if (!first) os_ << ",";
      first = false;
      ptr = col_printer(os_, ptr);
    }
    os_ << std::endl;
  }
}

}  // namespace tpl::sql
