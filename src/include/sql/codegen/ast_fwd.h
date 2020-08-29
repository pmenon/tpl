#pragma once

// Forward-declare a few of the common AST nodes here to reduce coupling between
// the SQL code generation components and the TPL AST systems.

namespace tpl::ast {

class BlockStmt;
class Context;
class Expr;
class Decl;
class FieldDecl;
class File;
class FunctionDecl;
class IdentifierExpr;
class Stmt;
class StructDecl;
class VariableDecl;

}  // namespace tpl::ast
