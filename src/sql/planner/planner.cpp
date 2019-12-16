// Dummy file to make sure all .h files compile.

#include "sql/planner/expressions/abstract_expression.h"
#include "sql/planner/expressions/aggregate_expression.h"
#include "sql/planner/expressions/column_value_expression.h"
#include "sql/planner/expressions/comparison_expression.h"
#include "sql/planner/expressions/conjunction_expression.h"
#include "sql/planner/expressions/constant_value_expression.h"
#include "sql/planner/expressions/derived_value_expression.h"
#include "sql/planner/expressions/expression_defs.h"
#include "sql/planner/expressions/operator_expression.h"

#include "sql/planner/plannodes/abstract_join_plan_node.h"
#include "sql/planner/plannodes/abstract_plan_node.h"
#include "sql/planner/plannodes/abstract_scan_plan_node.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/planner/plannodes/hash_join_plan_node.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
#include "sql/planner/plannodes/output_schema.h"
#include "sql/planner/plannodes/plan_node_defs.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/planner/plannodes/set_op_plan_node.h"

namespace tpl::sql::planner {

std::string PlanNodeTypeToString(PlanNodeType type) {
  switch (type) {
    case PlanNodeType::INVALID:
      return "Invalid";
    case PlanNodeType::SEQSCAN:
      return "SequentialScan";
    case PlanNodeType::INDEXSCAN:
      return "IndexScan";
    case PlanNodeType::CSVSCAN:
      return "CsvScan";
    case PlanNodeType::NESTLOOP:
      return "NestedLoop";
    case PlanNodeType::HASHJOIN:
      return "HashJoin";
    case PlanNodeType::INDEXNLJOIN:
      return "IndexNestedLoopJoin";
    case PlanNodeType::UPDATE:
      return "Update";
    case PlanNodeType::INSERT:
      return "Insert";
    case PlanNodeType::DELETE:
      return "Delete";
    case PlanNodeType::CREATE_DATABASE:
      return "CreateDatabase";
    case PlanNodeType::CREATE_NAMESPACE:
      return "CreateNamespace";
    case PlanNodeType::CREATE_TABLE:
      return "CreateTable";
    case PlanNodeType::CREATE_INDEX:
      return "CreateIndex";
    case PlanNodeType::CREATE_FUNC:
      return "CreateFunction";
    case PlanNodeType::CREATE_TRIGGER:
      return "CreateTrigger";
    case PlanNodeType::CREATE_VIEW:
      return "CreateView";
    case PlanNodeType::DROP_DATABASE:
      return "DropDatabase";
    case PlanNodeType::DROP_NAMESPACE:
      return "DropNamespace";
    case PlanNodeType::DROP_TABLE:
      return "DropTable";
    case PlanNodeType::DROP_INDEX:
      return "DropIndex";
    case PlanNodeType::DROP_TRIGGER:
      return "DropTrigger";
    case PlanNodeType::DROP_VIEW:
      return "DropView";
    case PlanNodeType::ANALYZE:
      return "Analyze";
    case PlanNodeType::AGGREGATE:
      return "Aggregate";
    case PlanNodeType::ORDERBY:
      return "OrderBy";
    case PlanNodeType::PROJECTION:
      return "Projection";
    case PlanNodeType::LIMIT:
      return "Limit";
    case PlanNodeType::DISTINCT:
      return "Distinct";
    case PlanNodeType::HASH:
      return "Hash";
    case PlanNodeType::SETOP:
      return "SetOperation";
    case PlanNodeType::EXPORT_EXTERNAL_FILE:
      return "ExportExternalFile";
    case PlanNodeType::RESULT:
      return "Result";
    case PlanNodeType::MOCK:
      return "Mock";
  }
  UNREACHABLE("Impossible to reach. All cases handled.");
}

}  // namespace tpl::sql::planner