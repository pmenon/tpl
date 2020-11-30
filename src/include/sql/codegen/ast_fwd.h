#pragma once

// Forward-declare a few of the common AST nodes here to reduce coupling between
// the SQL code generation components and the TPL AST systems.

namespace tpl::ast {

class BlockStatement;
class Context;
class Expression;
class Declaration;
class FieldDeclaration;
class File;
class FunctionDeclaration;
class IdentifierExpression;
class Statement;
class StructDeclaration;
class VariableDeclaration;

}  // namespace tpl::ast
