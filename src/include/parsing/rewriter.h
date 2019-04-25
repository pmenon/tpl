#pragma once

namespace tpl::ast {
class Context;
class ForInStmt;
class Stmt;
}  // namespace tpl::ast

namespace tpl::parsing {

ast::Stmt *RewriteForInScan(ast::Context *ctx, ast::ForInStmt *for_in);

}  // namespace tpl::parsing
