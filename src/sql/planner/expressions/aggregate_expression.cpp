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

TypeId AggregateExpression::DerivedReturnType(AggregateKind kind, TypeId input_type) {
  switch (kind) {
    case AggregateKind::COUNT:
    case AggregateKind::COUNT_STAR:
      return TypeId::BigInt;
    case AggregateKind::SUM:
    case AggregateKind::MIN:
    case AggregateKind::MAX:
      return input_type;
    case AggregateKind::AVG:
      return TypeId::Double;
  }
  UNREACHABLE("Impossible to reach. All aggregate kinds handled.");
}

}  // namespace tpl::sql::planner
