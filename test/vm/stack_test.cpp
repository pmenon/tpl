#include "tpl_test.h"

#include <numeric>
#include <random>
#include <unordered_set>

#include "vm/stack.h"

namespace tpl::vm::test {

class StackTest : public TplTest {};

TEST_F(StackTest, PushPopIntsOnly) {
  Stack s(10);

  EXPECT_EQ(10u, s.capacity());

  s.PushInt(44);

  EXPECT_EQ(1u, s.size());
  EXPECT_EQ(44, s.TopInt());

  auto first = s.PopInt();

  EXPECT_EQ(0u, s.size());
  EXPECT_EQ(44, first);
}

TEST_F(StackTest, PushPopMixedTypesOnly) {
  const uint32_t capacity = 10;
  Stack s(capacity);

  EXPECT_EQ(10u, s.capacity());

  s.PushInt(-44);
  s.PushFloat(float(123.0));
  s.PushInt(1000000);
  s.PushFloat(float(-321.0));
  s.PushReference(&s);

  EXPECT_EQ(5u, s.size());

  // Pop the stack reference first
  auto first = s.PopReference();
  EXPECT_EQ(4u, s.size());
  EXPECT_EQ(&s, first);

  // Pop the negative float
  auto second = s.PopFloat();
  EXPECT_EQ(3u, s.size());
  EXPECT_EQ(float(-321.0), second);

  // Pop the large integer
  auto fourth = s.PopInt();
  EXPECT_EQ(2u, s.size());
  EXPECT_EQ(1000000, fourth);

  // Pop the positive float
  auto fifth = s.PopFloat();
  EXPECT_EQ(1u, s.size());
  EXPECT_EQ(float(123.0), fifth);

  // Pop the last small integer
  auto sixth = s.PopInt();
  EXPECT_EQ(0u, s.size());
  EXPECT_EQ(-44, sixth);

  // Try to push again on empty
  s.PushInt(333);
  EXPECT_EQ(1u, s.size());
  EXPECT_EQ(333, s.PopInt());
  EXPECT_EQ(0u, s.size());
  EXPECT_EQ(capacity, s.capacity());
}

TEST_F(StackTest, TopTest) {
  const uint32_t capacity = 10;
  Stack s(capacity);

  EXPECT_EQ(10u, s.capacity());

  s.PushInt(-44);

  EXPECT_EQ(-44, s.TopInt());

  s.PushReference(&s);

  EXPECT_EQ(&s, s.TopReference());
}

TEST_F(StackTest, SetTopTest) {
  const uint32_t capacity = 10;
  Stack s(capacity);

  EXPECT_EQ(10u, s.capacity());

  s.PushInt(-44);

  EXPECT_EQ(-44, s.TopInt());

  s.SetTopReference(&s);

  EXPECT_EQ(&s, s.TopReference());

  auto ref = s.PopReference();
  EXPECT_EQ(&s, ref);
}

}  // namespace tpl::vm::test