#include "parsing/rewriter.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "util/macros.h"

namespace tpl::parsing {

namespace {

ast::Stmt *InitTVI(ast::Context *ctx, ast::Identifier tvi_name) {
  SourcePosition pos{0, 0};

  auto *factory = ctx->node_factory();

  auto *tvi_type =
      ast::BuiltinType::Get(ctx, ast::BuiltinType::TableVectorIterator);

  auto tvi_type_ident = ctx->GetIdentifier(tvi_type->tpl_name());

  return factory->NewDeclStmt(factory->NewVariableDecl(
      pos, tvi_name, factory->NewIdentifierExpr(pos, tvi_type_ident), nullptr));
}

}  // namespace

ast::Stmt *RewriteForInScans(ast::Context *ctx, ast::ForInStmt *for_in) {
  // We want to convert the following:
  //
  // for ('target' in 'iter') {
  //   body
  // }
  //
  // into:
  //
  // var $tvi: TableVectorIterator
  // for (@tableIterInit(&$tvi, 'iter'); @tableIterAdvance(&$tvi); ) {
  //   var 'target' = @tableIterGetVPI(&$tvi)
  //   (body)
  // }
  // @tableIterClose(&$tvi)
  //

  const auto pos = for_in->position();
  auto *factory = ctx->node_factory();

  util::RegionVector<ast::Stmt *> statements(ctx->region());

  // The iterator's name
  ast::Identifier tvi_name = ctx->GetIdentifier("$tvi");
  // An expression computing the address of the iterator. Used all over here.
  ast::Expr *tvi_addr = factory->NewUnaryOpExpr(
      pos, Token::Type::AMPERSAND, factory->NewIdentifierExpr(pos, tvi_name));

  // Declare the TVI
  statements.push_back(InitTVI(ctx, tvi_name));

  // Now the for-loop
  ast::Stmt *init = nullptr, *next = nullptr;
  ast::Expr *cond = nullptr;

  // First the initialization
  {
    util::RegionVector<ast::Expr *> args({tvi_addr, for_in->target()},
                                         ctx->region());
    auto *name = factory->NewIdentifierExpr(
        pos, ctx->GetIdentifier(
                 ast::Builtins::GetFunctionName(ast::Builtin::TableIterInit)));
    auto *call = factory->NewBuiltinCallExpr(name, std::move(args));
    init = factory->NewExpressionStmt(call);
  }

  // Next, the condition
  {
    util::RegionVector<ast::Expr *> args({tvi_addr}, ctx->region());
    auto *name = factory->NewIdentifierExpr(
        pos, ctx->GetIdentifier(ast::Builtins::GetFunctionName(
                 ast::Builtin::TableIterAdvance)));
    cond = factory->NewBuiltinCallExpr(name, std::move(args));
  }

  // Splice in the target initialization at the start of the body
  {
    util::RegionVector<ast::Expr *> args({tvi_addr}, ctx->region());
    auto *name = factory->NewIdentifierExpr(
        pos, ctx->GetIdentifier(ast::Builtins::GetFunctionName(
                 ast::Builtin::TableIterGetVPI)));
    auto *call = factory->NewBuiltinCallExpr(name, std::move(args));
    auto *vpi_decl = factory->NewVariableDecl(
        pos, for_in->target()->As<ast::IdentifierExpr>()->name(), nullptr,
        call);
    auto &body = for_in->body()->statements();
    body.insert(body.begin(), factory->NewDeclStmt(vpi_decl));
  }

  // Add the loop to the running statements list
  statements.push_back(
      factory->NewForStmt(pos, init, cond, next, for_in->body()));

  // Close the iterator after the loop
  {
    util::RegionVector<ast::Expr *> args({tvi_addr}, ctx->region());
    auto *name = factory->NewIdentifierExpr(
        pos, ctx->GetIdentifier(
                 ast::Builtins::GetFunctionName(ast::Builtin::TableIterClose)));
    auto *call = factory->NewBuiltinCallExpr(name, std::move(args));
    statements.push_back(factory->NewExpressionStmt(call));
  }

  // Done
  return ctx->node_factory()->NewBlockStmt(pos, pos, std::move(statements));
}

}  // namespace tpl::parsing
