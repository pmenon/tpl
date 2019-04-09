#pragma once
#include <stdlib.h>
#include <string>

#include "compiler/AbstractCompiler.h"

#include "parser/expression/abstract_expression.h"

namespace tpl::compiler {

class TextCompiler : public AbstractCompiler {

  void CompileAndRun() override;

  std::string CompilePredicate(const terrier::parser::AbstractExpression &expression);
};

}  // namespace tpl::compiler