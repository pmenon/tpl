#pragma once
#include <stdlib.h>
#include <string>

#include "compiler/AbstractCompiler.h"

#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {

class TextCompiler : public AbstractCompiler {

  void CompileAndRun() override;

  std::string CompilePredicate(const std::shared_ptr<terrier::parser::AbstractExpression> expression);

 private:
  void CompileSubPredicate(const std::shared_ptr<terrier::parser::AbstractExpression> expression, std::stringstream *stream);

};

}  // namespace tpl::compiler