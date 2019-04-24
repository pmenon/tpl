#pragma once

namespace tpl::ast {
class AstNode;
class Context;
}  // namespace tpl::ast

namespace tpl::vm {

void RewriteForInScans(ast::Context *ctx, ast::AstNode *root);

}  // namespace tpl::vm
