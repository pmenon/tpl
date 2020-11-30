#pragma once

#include <string>
#include <utility>

#include "ast/identifier.h"
#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "util/region_containers.h"

namespace tpl::sql::codegen {

class CodeGen;

/**
 * Helper class to build TPL functions.
 */
class FunctionBuilder {
  friend class If;
  friend class Loop;

 public:
  /**
   * Create a builder for a function with the provided name, return type, and arguments.
   * @param codegen The code generation instance.
   * @param name The name of the function.
   * @param params The parameters to the function.
   * @param ret_type The return type representation of the function.
   */
  FunctionBuilder(CodeGen *codegen, ast::Identifier name,
                  util::RegionVector<ast::FieldDeclaration *> &&params, ast::Expr *ret_type);

  /**
   * Destructor.
   */
  ~FunctionBuilder();

  /**
   * @return A reference to a function parameter by its ordinal position.
   */
  ast::Expr *GetParameterByPosition(uint32_t param_idx);

  /**
   * Append a statement to the list of statements in this function.
   * @param stmt The statement to append.
   */
  void Append(ast::Statement *stmt);

  /**
   * Append an expression as a statement to the list of statements in this function.
   * @param expr The expression to append as a statement.
   */
  void Append(ast::Expr *expr);

  /**
   * Append a variable declaration as a statement to the list of statements in this function.
   * @param decl The declaration to append to the statement.
   */
  void Append(ast::VariableDeclaration *decl);

  /**
   * Finish constructing the function.
   * @param ret The value to return from the function. Use a null pointer to return nothing.
   * @return The build function declaration.
   */
  ast::FunctionDeclaration *Finish(ast::Expr *ret = nullptr);

  /**
   * @return The final constructed function; null if the builder hasn't been constructed through
   *         FunctionBuilder::Finish().
   */
  ast::FunctionDeclaration *GetConstructedFunction() const { return decl_; }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const { return codegen_; }

 private:
  // The code generation instance.
  CodeGen *codegen_;
  // The previously active function.
  FunctionBuilder *prev_function_;
  // The function's name.
  ast::Identifier name_;
  // The function's arguments.
  util::RegionVector<ast::FieldDeclaration *> params_;
  // The return type of the function.
  ast::Expr *ret_type_;
  // The start and stop position of statements in the function.
  SourcePosition start_;
  // The list of generated statements making up the function.
  ast::BlockStatement *statements_;
  // The cached function declaration. Constructed once in Finish().
  ast::FunctionDeclaration *decl_;
};

}  // namespace tpl::sql::codegen
