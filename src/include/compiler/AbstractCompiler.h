#pragma once

namespace tpl::compiler {

class AbstractCompiler {
  virtual void CompileAndRun() = 0;
};

}  // namespace tpl::compiler