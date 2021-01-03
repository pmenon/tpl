#pragma once

#include <string>
#include <utility>

#include "ast/identifier.h"
#include "common/common.h"
#include "sql/codegen/ast_fwd.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"
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
  using Param = std::pair<ast::Identifier, ast::Type *>;

  /**
   * Create a builder for a function with the provided name, return type, and arguments.
   * @param codegen The code generation instance.
   * @param name The name of the function.
   * @param params The parameters to the function.
   * @param ret_type The return type representation of the function.
   */
  FunctionBuilder(CodeGen *codegen, ast::Identifier name, std::vector<Param> &&params,
                  ast::Type *ret_type);

  /**
   * Destructor.
   */
  ~FunctionBuilder();

  /**
   * @return A reference to a function parameter by its ordinal position.
   */
  edsl::ValueVT GetParameterByPosition(uint32_t param_idx) const;

  /**
   * @return The number of parameters this function takes.
   */
  std::size_t GetParameterCount() const noexcept { return params_.size(); }

  /**
   * Append a statement to the list of statements in this function.
   * @param stmt The statement to append.
   */
  void Append(const edsl::Value<void> &stmt);

  /**
   * Finish constructing the function.
   * @param ret The value to return from the function. Use a null pointer to return nothing.
   * @return The build function declaration.
   */
  ast::FunctionDeclaration *Finish();

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
  std::vector<edsl::VariableVT> params_;
  // The return type of the function.
  ast::Type *ret_type_;
  // The start and stop position of statements in the function.
  SourcePosition start_;
  // The list of generated statements making up the function.
  ast::BlockStatement *statements_;
  // The cached function declaration. Constructed once in Finish().
  ast::FunctionDeclaration *decl_;
};

}  // namespace tpl::sql::codegen
