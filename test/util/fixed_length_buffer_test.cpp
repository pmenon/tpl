#include <algorithm>
#include <deque>
#include <random>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "util/fixed_length_buffer.h"

namespace tpl::util::test {

class FixedLengthBufferTest : public TplTest {};

TEST_F(FixedLengthBufferTest, Append) {
  FixedLengthBuffer<u32, 10> buffer;

  EXPECT_TRUE(buffer.empty());
  buffer.append(200u);
  EXPECT_FALSE(buffer.empty());
  EXPECT_EQ(1u, buffer.size());

  EXPECT_EQ(200u, buffer[0]);

  buffer.clear();
  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(0u, buffer.size());
}

TEST_F(FixedLengthBufferTest, Iteration) {
  constexpr u32 nelems = 100;

  std::vector<u32> reference;
  FixedLengthBuffer<u32, nelems> buffer;

  std::random_device r;
  for (u32 i = 0; i < nelems; i++) {
    u32 num = r();
    buffer.append(num);
    reference.push_back(num);
    EXPECT_EQ(num, buffer[i]);
  }

  auto ref_iter = reference.begin();
  for (auto buf_elem : buffer) {
    EXPECT_EQ(*ref_iter++, buf_elem);
  }
}

TEST_F(FixedLengthBufferTest, OutOfBoundsAccess) {
  FixedLengthBuffer<u32, 2> buffer;

  EXPECT_THROW(buffer.at(0), std::out_of_range);

  buffer.append(10);
  EXPECT_NO_THROW(buffer.at(0));
  EXPECT_EQ(10u, buffer[0]);
  EXPECT_THROW(buffer.at(1), std::out_of_range);

  buffer.append(11);
  EXPECT_NO_THROW(buffer.at(0));
  EXPECT_NO_THROW(buffer.at(1));
  EXPECT_EQ(10u, buffer[0]);
  EXPECT_EQ(11u, buffer[1]);
  EXPECT_THROW(buffer.at(2), std::out_of_range);
}

}  // namespace tpl::util::test
