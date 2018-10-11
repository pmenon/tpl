#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"
#include "runtime/sql_table.h"

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

ast::Type *SqlTypeToTplType(ast::AstContext &ctx, runtime::SqlTypeId type) {
  switch (type) {
    case runtime::SqlTypeId::SmallInt: {
      return ast::IntegerType::Int16(ctx);
    }
    case runtime::SqlTypeId::Integer: {
      return ast::IntegerType::Int32(ctx);
    }
    case runtime::SqlTypeId::BigInt: {
      return ast::IntegerType::Int32(ctx);
    }
    case runtime::SqlTypeId::Decimal: {
      return ast::FloatType::Float64(ctx);
    }
  }
  UNREACHABLE("Impossible SQL type");
}

}  // namespace

ast::Type *Sema::ConvertToType(const runtime::Schema &schema) {
  util::RegionVector<ast::Field> cols(ast_context().region());
  for (const auto &col : schema.cols) {
    auto col_name = ast_context().GetIdentifier(col.name);
    auto col_type = SqlTypeToTplType(ast_context(), col.type_id);
    cols.emplace_back(col_name, col_type);
  }
  return ast::StructType::Get(ast_context(), std::move(cols));
}

}  // namespace tpl::sema