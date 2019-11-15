#include <algorithm>
#include <deque>
#include <random>
#include <utility>
#include <vector>

#include "util/fixed_length_buffer.h"
#include "util/test_harness.h"

namespace tpl::util {

class FixedLengthBufferTest : public TplTest {};

TEST_F(FixedLengthBufferTest, Append) {
  FixedLengthBuffer<uint32_t, 10> buffer;

  EXPECT_TRUE(buffer.IsEmpty());
  buffer.Append(200u);
  EXPECT_FALSE(buffer.IsEmpty());
  EXPECT_EQ(1u, buffer.GetSize());

  EXPECT_EQ(200u, buffer[0]);

  buffer.Clear();
  EXPECT_TRUE(buffer.IsEmpty());
  EXPECT_EQ(0u, buffer.GetSize());
}

TEST_F(FixedLengthBufferTest, Iteration) {
  constexpr uint32_t nelems = 100;

  std::vector<uint32_t> reference;
  FixedLengthBuffer<uint32_t, nelems> buffer;

  std::random_device r;
  for (uint32_t i = 0; i < nelems; i++) {
    uint32_t num = r();
    buffer.Append(num);
    reference.push_back(num);
    EXPECT_EQ(num, buffer[i]);
  }

  auto ref_iter = reference.begin();
  for (auto buf_elem : buffer) {
    EXPECT_EQ(*ref_iter++, buf_elem);
  }
}

TEST_F(FixedLengthBufferTest, OutOfBoundsAccess) {
  FixedLengthBuffer<uint32_t, 2> buffer;

  EXPECT_THROW(buffer.At(0), std::out_of_range);

  buffer.Append(10);
  EXPECT_NO_THROW(buffer.At(0));
  EXPECT_EQ(10u, buffer[0]);
  EXPECT_THROW(buffer.At(1), std::out_of_range);

  buffer.Append(11);
  EXPECT_NO_THROW(buffer.At(0));
  EXPECT_NO_THROW(buffer.At(1));
  EXPECT_EQ(10u, buffer[0]);
  EXPECT_EQ(11u, buffer[1]);
  EXPECT_THROW(buffer.At(2), std::out_of_range);
}

}  // namespace tpl::util
