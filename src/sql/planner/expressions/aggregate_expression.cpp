#include "sql/planner/expressions/aggregate_expression.h"

#include <vector>
#include "sql/planner/expressions/abstract_expression.h"

namespace tpl::sql::planner {

AggregateExpression::AggregateExpression(AggregateKind kind,
                                         std::vector<const AbstractExpression *> &&children,
                                         bool distinct)
    : AbstractExpression(ExpressionType::AGGREGATE,
                         DerivedReturnType(kind, children[0]->GetReturnValueType()),
                         std::move(children)),
      kind_(kind),
      distinct_(distinct) {}

Type AggregateExpression::DerivedReturnType(AggregateKind kind, const Type &input_type) {
  switch (kind) {
    case AggregateKind::COUNT:
    case AggregateKind::COUNT_STAR:
      return Type::BigIntType(false);
    case AggregateKind::SUM:
    case AggregateKind::MIN:
    case AggregateKind::MAX:
      return input_type;
    case AggregateKind::AVG:
      return Type::DoubleType(input_type.IsNullable());
  }
  UNREACHABLE("Impossible to reach. All aggregate kinds handled.");
}

}  // namespace tpl::sql::planner
