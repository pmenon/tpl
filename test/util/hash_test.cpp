#include <string>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "util/hash.h"

namespace tpl::util::test {

class HashTest : public TplTest {};

TEST_F(HashTest, MethodsProduceEqualHash) {
  const auto small_input = "This is a kinda long string";
  const auto i16_input = i16(-20);
  const auto i32_input = i32(-400);
  const auto i64_input = i64(-8000);
  const auto u16_input = u16(20);
  const auto u32_input = u32(400);
  const auto u64_input = u64(8000);

  auto large_input = std::string();
  for (u32 i = 0; i < 40; i++) {
    large_input += small_input;
  }

  std::vector<std::pair<const u8 *, u32>> inputs = {
      {reinterpret_cast<const u8 *>(small_input), strlen(small_input)},
      {reinterpret_cast<const u8 *>(large_input.c_str()), large_input.length()},
      {reinterpret_cast<const u8 *>(&i16_input), sizeof(i16_input)},
      {reinterpret_cast<const u8 *>(&i32_input), sizeof(i32_input)},
      {reinterpret_cast<const u8 *>(&i64_input), sizeof(i64_input)},
      {reinterpret_cast<const u8 *>(&u16_input), sizeof(u16_input)},
      {reinterpret_cast<const u8 *>(&u32_input), sizeof(u32_input)},
      {reinterpret_cast<const u8 *>(&u64_input), sizeof(u64_input)},
  };

  for (const auto method :
       {HashMethod::Fnv1, HashMethod::Crc, HashMethod::xxHash3}) {
    for (const auto [input_buf, input_len] : inputs) {
      auto hash_val1 = Hasher::Hash(input_buf, input_len, method);
      auto hash_val2 = Hasher::Hash(input_buf, input_len, method);
      EXPECT_EQ(hash_val1, hash_val2);
    }
  }
}

}  // namespace tpl::util::test
