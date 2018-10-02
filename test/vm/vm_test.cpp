#include "gtest/gtest.h"

#include "tpl_test.h"

#include "vm/bytecode_emitter.h"
#include "vm/bytecode_function.h"
#include "vm/vm.h"

namespace tpl::vm::test {

class VmTest : public TplTest {
 protected:
  VmTest() : builder_(std::make_unique<BytecodeEmitter>()) {}

  BytecodeEmitter &builder() { return *builder_; }

 private:
  std::unique_ptr<BytecodeEmitter> builder_;
};

TEST_F(VmTest, Simple) {
  // 1 + 2
  auto func = builder()
                  .LoadLiteral(1)
                  .LoadLiteral(2)
                  .BinaryOperation(parsing::Token::Type::PLUS)
                  .LoadLiteral(2)
                  .BinaryOperation(parsing::Token::Type::MINUS)
                  .Return()
                  .Build();

  UNUSED auto ret = VM::Invoke(*func);
}

}  // namespace tpl::vm::test