#include "sql/operators/cast_operators.h"

#include "util/fast_double_parser.h"

namespace tpl::sql {

bool TryCast<VarlenEntry, float>::operator()(const VarlenEntry &input, float *output) const {
  double double_output;
  if (!TryCast<VarlenEntry, double>{}(input, &double_output)) {
    return false;
  }
  return TryCast<double, float>{}(double_output, output);
}

bool TryCast<VarlenEntry, double>::operator()(const VarlenEntry &input, double *output) const {
  const auto buf = reinterpret_cast<const char *>(input.GetContent());
  return util::fast_double_parser::parse_number(buf, output);
}

}  // namespace tpl::sql
