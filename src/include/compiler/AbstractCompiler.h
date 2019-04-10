#pragma once

namespace tpl::compiler {

class AbstractCompiler {
 public:
  virtual void CompileAndRun() = 0;
};

}  // namespace tpl::compiler