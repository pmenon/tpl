#include "gtest/gtest.h"

#include "tpl_test.h"

#include "vm/bytecode_builder.h"

namespace tpl::vm::test {

class BytecodeBuilderTest : public TplTest {
 protected:
  BytecodeBuilderTest()
      : builder_(std::make_unique<BytecodeBuilder>(
            std::make_unique<ConstantsArrayBuilder>())) {}

  BytecodeBuilder &builder() { return *builder_; }

 private:
  std::unique_ptr<BytecodeBuilder> builder_;
};

TEST_F(BytecodeBuilderTest, LoadConstantTest) {
  // 1 + 2
  builder().LoadLiteral(1).LoadLiteral(2).BinaryOperation(
      parsing::Token::Type::PLUS);
}

}  // namespace tpl::vm::test