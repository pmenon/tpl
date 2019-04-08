#include "sema/sema.h"

#include <utility>

#include "ast/context.h"
#include "ast/type.h"
#include "sql/data_types.h"
#include "sql/schema.h"
#include "sql/table.h"

namespace tpl::sema {

Sema::Sema(ast::Context *ctx)
    : ctx_(ctx),
      error_reporter_(ctx->error_reporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {}

// Main entry point to semantic analysis and type checking an AST
bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter()->HasErrors();
}

ast::Type *Sema::GetRowTypeFromSqlSchema(const sql::Schema &schema) {
  util::RegionVector<ast::Field> cols(context()->region());
  for (const auto &col : schema.columns()) {
    auto col_name = context()->GetIdentifier(col.name);
    auto *col_type = context()->GetTplTypeFromSqlType(col.type);
    cols.emplace_back(col_name, col_type);
  }
  return ast::StructType::Get(context(), std::move(cols));
}

}  // namespace tpl::sema
