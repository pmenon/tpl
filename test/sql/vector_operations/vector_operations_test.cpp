#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/constant_vector.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/fast_rand.h"

namespace tpl::sql {

class VectorOperationsTest : public TplTest {};

TEST_F(VectorOperationsTest, BLAH) {}

#if 0
TEST_F(VectorOperationsTest, PerfIteration) {
  constexpr u32 nruns = 5000;

  alignas(64) u8 vec_1[kDefaultVectorSize], vec_2[kDefaultVectorSize],
      vec_3[kDefaultVectorSize], vec_4[kDefaultVectorSize];
  ;

  u16 sel_vec[kDefaultVectorSize];

  for (f64 sel :
       {0.005, 0.009, 0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.25, 0.4,  0.45,
        0.5,   0.55,  0.6,  0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99, 1.0}) {
    u32 num_selected = std::round(sel * kDefaultVectorSize);

    TupleIdList tid_list;

    std::random_device r;
    std::bitset<kDefaultVectorSize> set;

    // Compute selection vector
    while (set.count() < num_selected) {
      auto idx = r() % kDefaultVectorSize;
      if (!set[idx]) {
        set[idx] = true;
      }
    }

    // Populate selection vector
    for (u32 i = 0, idx = 0; i < kDefaultVectorSize; i++) {
      if (set[i]) {
        sel_vec[idx] = i;
        tid_list.Add(i);
        idx++;
      }
    }

    const auto bound = u64(std::lround(sel * 1000.0));
    for (u32 i = 0; i < kDefaultVectorSize; i++) {
      vec_1[i] = vec_2[i] = r() % 15;
      if (r() % 1000 < bound) vec_2[i]++;
    }

    auto tid_list_ms = Bench(5, [&]() {
      for (u32 run = 0; run < nruns; run++) {
        GetTypeIdSize(TypeId::Hash);
        for (const auto idx : tid_list) {
          vec_3[idx] = vec_1[idx] + vec_2[idx];
        }
      }
    });

    auto sel_vec_ms = Bench(5, [&]() {
      for (u32 run = 0; run < nruns; run++) {
        VectorOps::Exec(sel_vec, num_selected,
                        [&](u64 i, u64 k) { vec_4[i] = vec_1[i] + vec_2[i]; });
      }
    });

    VectorOps::Exec(sel_vec, num_selected,
                    [&](u64 i, u64 k) { EXPECT_EQ(vec_4[i], vec_3[i]); });

    LOG_INFO("{:>3.0f},{:>8.2f},{:>8.2f}", sel * 100.0, tid_list_ms,
             sel_vec_ms);
  }
}
#endif

}  // namespace tpl::sql
