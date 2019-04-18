#pragma once
#include <stdlib.h>
#include <string>

#include "compiler/AbstractCompiler.h"

#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {

#define DEFAULT_ROW_NAME "row"

class TextCompiler : public AbstractCompiler {

 public:
  void CompileAndRun() override;

  std::string CompilePredicate(
      const std::shared_ptr<tpl::parser::AbstractExpression> expression,
      const std::string &rowName);

 private:
  void CompileSubPredicate(
      std::shared_ptr<const tpl::parser::AbstractExpression> expression,
      const std::string &rowName, std::stringstream *stream);
};

}  // namespace tpl::compiler