#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"
#include "sql/table.h"
#include "sql/schema.h"
#include "sql/type.h"

namespace tpl::sema {

Sema::Sema(ast::AstContext &ctx)
    : ctx_(ctx),
      error_reporter_(ctx.error_reporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {}

// Main entry point to semantic analysis and type checking an AST
bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

namespace {

ast::Type *SqlTypeToTplType(ast::AstContext &ctx, const sql::Type &type) {
  ast::InternalType::InternalKind tpl_type;
  if (type.IsIntegral()) {
    tpl_type = ast::InternalType::InternalKind::SqlInteger;
  } else if (type.IsDecimal()) {
    tpl_type = ast::InternalType::InternalKind::SqlDecimal;
  } else {
    UNREACHABLE("Impossible type");
  }
  return ast::InternalType::Get(ctx, tpl_type);
}

}  // namespace

ast::Type *Sema::ConvertToType(const sql::Schema &schema) {
  util::RegionVector<ast::Field> cols(ast_context().region());
  for (const auto &col : schema.columns()) {
    auto col_name = ast_context().GetIdentifier(col.name);
    auto col_type = SqlTypeToTplType(ast_context(), col.type);
    cols.emplace_back(col_name, col_type);
  }
  return ast::StructType::Get(ast_context(), std::move(cols));
}

}  // namespace tpl::sema