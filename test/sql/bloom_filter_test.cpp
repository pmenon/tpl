#include "tpl_test.h"

#include <random>
#include <unordered_set>

#include "sql/bloom_filter.h"
#include "util/hash.h"

namespace tpl::sql::test {

class BloomFilterTest : public TplTest {};

template <typename F>
void GenerateRandom32(std::vector<u32> &vals, u32 n, const F &f) {
  vals.resize(n);
  std::random_device random;
  auto genrand = [&random, &f]() {
    while (true) {
      auto r = random();
      if (f(r)) {
        return r;
      }
    }
  };
  std::generate(vals.begin(), vals.end(), genrand);
}

void GenerateRandom32(std::vector<u32> &vals, u32 n) {
  GenerateRandom32(vals, n, [](auto r) { return true; });
}

// Mix in elements from source into the target vector with probability p
template <typename T>
void Mix(std::vector<T> &target, const std::vector<T> &source, double p) {
  TPL_ASSERT(target.size() > source.size(), "Bad sizes!");
  std::random_device random;
  std::mt19937 g(random());

  for (u32 i = 0; i < (p * target.size()); i++) {
    target[i] = source[g() % source.size()];
  }

  std::shuffle(target.begin(), target.end(), g);
}

TEST_F(BloomFilterTest, ComprehensiveTest) {
  const u32 num_filter_elems = 10000;
  const u32 lookup_scale_factor = 100;

  // Create a vector of data to insert into the filter
  std::vector<u32> insertions;
  GenerateRandom32(insertions, num_filter_elems);

  // The validation set. We use this to check false negatives.
  std::unordered_set<u32> check(insertions.begin(), insertions.end());

  util::Region tmp("filter");
  BloomFilter filter(&tmp, num_filter_elems);
  for (const auto elem : insertions) {
    filter.Add(util::Hasher::Hash((const u8 *)&elem, sizeof(elem)));
  }

  // All inserted elements **must** be present in filter
  for (const auto elem : insertions) {
    filter.Add(util::Hasher::Hash((const u8 *)&elem, sizeof(elem)));
  }

  double bits_per_elem = (double)filter.GetSizeInBits() / num_filter_elems;
  double bit_set_prob =
      (double)filter.GetTotalBitsSet() / filter.GetSizeInBits();
  LOG_INFO(
      "Filter: {} elements, {} bits, {} bits/element, {} bits set (p={:.2f})",
      num_filter_elems, filter.GetSizeInBits(), bits_per_elem,
      filter.GetTotalBitsSet(), bit_set_prob);

  for (auto prob_success : {0.00, 0.25, 0.50, 0.75, 1.00}) {
    std::vector<u32> lookups;
    GenerateRandom32(lookups, num_filter_elems * lookup_scale_factor);
    Mix(lookups, insertions, prob_success);

    u32 expected_found = static_cast<u32>(prob_success * lookups.size());

    util::Timer<std::milli> timer;
    timer.Start();

    u32 actual_found = 0;
    for (const auto elem : lookups) {
      auto exists = filter.Contains(util::Hasher::Hash(
          reinterpret_cast<const u8 *>(&elem), sizeof(elem)));

      if (!exists) {
        EXPECT_EQ(0u, check.count(elem));
      }

      actual_found += static_cast<u32>(exists);
    }

    timer.Stop();

    double fpr = (actual_found - expected_found) / (double)lookups.size();
    double probes_per_sec =
        (double)lookups.size() / timer.elapsed() * 1000.0 / 1000000.0;
    LOG_INFO(
        "p: {:.2f}, {} M probes/sec, FPR: {:2.4f}, (expected: {}, actual: {})",
        prob_success, probes_per_sec, fpr, expected_found, actual_found);
  }
}

}  // namespace tpl::sql::test
