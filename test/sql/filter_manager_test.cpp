#include <algorithm>
#include <chrono>
#include <fstream>
#include <string>
#include <vector>

#include "gmock/gmock.h"

#include "sql/catalog.h"
#include "sql/filter_manager.h"
#include "sql/table_vector_iterator.h"
#include "sql/vector_filter_executor.h"
#include "util/test_harness.h"

namespace tpl::sql {

class FilterManagerTest : public TplTest {};

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

using namespace std::chrono_literals;  // NOLINT

TEST_F(FilterManagerTest, ConjunctionTest) {
  // Create a filter that implements: colA < 500 AND colB < 9
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms({[](auto vp, auto tids, auto ctx) {
                              VectorFilterExecutor::SelectLessThanVal(
                                  vp, Col::A, GenericValue::CreateInteger(500), tids);
                            },
                            [](auto vp, auto tids, auto ctx) {
                              VectorFilterExecutor::SelectLessThanVal(
                                  vp, Col::B, GenericValue::CreateInteger(9), tids);
                            }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

TEST_F(FilterManagerTest, DisjunctionTest) {
  // Create a filter that implements: colA < 500 OR colB < 9
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto vp, auto tids, auto ctx) {
    VectorFilterExecutor::SelectLessThanVal(vp, Col::A, GenericValue::CreateInteger(500), tids);
  });
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto vp, auto tids, auto ctx) {
    VectorFilterExecutor::SelectLessThanVal(vp, Col::B, GenericValue::CreateInteger(9), tids);
  });

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 || colb < 9);
  });
}

TEST_F(FilterManagerTest, MixedTaatVaatFilterTest) {
  // Create a filter that implements: colA < 500 AND colB < 9
  // The filter on column colB is implemented using a tuple-at-a-time filter.
  // Thus, the filter is a mixed VaaT and TaaT filter.
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms(
      {[](auto vp, auto tids, auto ctx) {
         VectorFilterExecutor::SelectLessThanVal(vp, Col::A, GenericValue::CreateInteger(500),
                                                 tids);
       },
       [](auto vp, auto tids, auto ctx) {
         VectorProjectionIterator iter(vp, tids);
         iter.RunFilter([&]() { return *iter.GetValue<int32_t, false>(Col::B, nullptr) < 9; });
       }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

TEST_F(FilterManagerTest, AdaptiveCheckTest) {
  uint32_t iter = 0;

  // Create a filter that implements: colA < 500 AND colB < 9
  FilterManager filter(true, &iter);
  filter.StartNewClause();
  filter.InsertClauseTerms({[](auto vp, auto tids, auto ctx) {
                              auto *r = reinterpret_cast<uint32_t *>(ctx);
                              if (*r < 100) std::this_thread::sleep_for(1us);  // Fake a sleep.
                              const auto val = GenericValue::CreateInteger(500);
                              VectorFilterExecutor::SelectLessThanVal(vp, Col::A, val, tids);
                            },
                            [](auto vp, auto tids, auto ctx) {
                              auto *r = reinterpret_cast<uint32_t *>(ctx);
                              if (*r > 100) std::this_thread::sleep_for(1us);  // Fake a sleep.
                              const auto val = GenericValue::CreateInteger(9);
                              VectorFilterExecutor::SelectLessThanVal(vp, Col::B, val, tids);
                            }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  for (uint32_t run = 0; run < 2; run++) {
    for (uint32_t i = 0; i < 100; i++, iter++) {
      // Remove any lingering filter.
      vp.Reset(kDefaultVectorSize);
      // Create an iterator and filter it.
      VectorProjectionIterator vpi(&vp);
      filter.RunFilters(&vpi);
    }

    // After a while, at least one re sampling should have occurred. At that time,
    // the manager should have realized that the second filter is more selective
    // and runs faster. When we switch to the second run, the second filter is
    // hobbled and the order should reverse back.
    EXPECT_EQ(1, filter.GetClauseCount());
    const auto clause = filter.GetOptimalClauseOrder()[0];
    EXPECT_GT(clause->GetResampleCount(), 1);
    if (run == 0) {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(1, 0));
    } else {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(0, 1));
    }
  }
}

void RunExperiment(bool adapt, const std::vector<FilterManager::MatchFn> &terms,
                   std::vector<double> *results) {
  FilterManager filter(adapt);
  filter.StartNewClause();
  filter.InsertClauseTerms(terms);

  TableVectorIterator tvi(static_cast<uint16_t>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto vpi = tvi.GetVectorProjectionIterator();
    const auto exec_micros = util::Time<std::micro>([&] { filter.RunFilters(vpi); });
    results->push_back(exec_micros);
  }
}

template <int32_t Min, int32_t Max>
void RunExperiment(const std::string &output) {
  // ALL terms here.
  std::vector<FilterManager::MatchFn> terms = {
      // c <= 6048
      [](auto input, auto tids, auto ctx) {
        const auto v = GenericValue::CreateInteger(6048);
        VectorFilterExecutor::SelectLessThanEqualVal(input, 2, v, tids);
      },
      // a >= MIN
      [](auto input, auto tids, auto ctx) {
        const auto val = GenericValue::CreateInteger(Min);
        VectorFilterExecutor::SelectGreaterThanEqualVal(input, 0, val, tids);
      },
      // b >= 2
      [](auto input, auto tids, auto ctx) {
        const auto v = GenericValue::CreateInteger(2);
        VectorFilterExecutor::SelectGreaterThanEqualVal(input, 1, v, tids);
      },
      // a < MAX
      [](auto input, auto tids, auto ctx) {
        const auto val = GenericValue::CreateInteger(Max);
        VectorFilterExecutor::SelectLessThanVal(input, 0, val, tids);
      },
      // b < 5
      [](auto input, auto tids, auto ctx) {
        const auto v = GenericValue::CreateInteger(5);
        VectorFilterExecutor::SelectLessThanVal(input, 1, v, tids);
      },
  };
  std::sort(terms.begin(), terms.end());

  std::vector<std::vector<double>> results;  // Subset of interesting results.
  std::vector<double> adapt_results;         // Results from adaptive filter.

  {
    // 'all_results' holds, for each possible ordering, the timings for each
    // partition. We time and capture all orderings because we don't know
    // which will be best ahead of time. So, we'll run them all, sort them
    // and choose the ones we want.
    std::vector<std::pair<uint32_t, std::vector<double>>> all_results;

    // Perform experiment on all possible orderings.
    uint32_t order_num = 0;
    do {
      std::vector<double> perm;
      RunExperiment(false, terms, &perm);
      all_results.emplace_back(order_num++, std::move(perm));
    } while (std::next_permutation(terms.begin(), terms.end()));

    // Run adaptive mode.
    RunExperiment(true, terms, &adapt_results);

    // Sort the results by time. 'all_results[0]' will be best.
    std::sort(all_results.begin(), all_results.end(), [](const auto &a, const auto &b) {
      auto a_total = std::accumulate(a.second.begin(), a.second.end(), 0.0);
      auto b_total = std::accumulate(b.second.begin(), b.second.end(), 0.0);
      return a_total <= b_total;
    });

    const auto num_orders = all_results.size();

    // Collect a subset of all the order timings.
    results.push_back(all_results[0].second);                   // Best
    results.push_back(all_results[num_orders / 2].second);      // 50%
    results.push_back(all_results[3 * num_orders / 4].second);  // 75%
    results.push_back(all_results.back().second);               // Worst
  }

  // Print results.
  const auto num_orders = results.size();
  const auto num_parts = results[0].size();

  // Print CSV of per-partition timings.
  {
    std::ofstream out_file;
    out_file.open(output.c_str());

    out_file << "Partition, ";
    for (uint32_t order = 0; order < num_orders; order++) {
      out_file << "Order_" << order << ", ";
    }
    out_file << "Adaptive" << std::endl;

    for (uint32_t i = 0; i < num_parts; i++) {
      out_file << i << ", ";
      std::cout << std::fixed << std::setprecision(5);
      for (uint32_t j = 0; j < num_orders; j++) {
        out_file << results[j][i] << ", ";
      }
      out_file << adapt_results[i] << std::endl;
    }
  }
}

TEST_F(FilterManagerTest, DISABLED_Experiment) {
  static constexpr uint32_t num_elems = 2000000;
  static constexpr uint32_t half = num_elems / 2;
  static constexpr uint32_t ten_pct = num_elems / 10;
  static constexpr uint32_t half_ten_pct = ten_pct / 2;

  RunExperiment<half, half>("filter-0.csv");
  RunExperiment<half - half_ten_pct, half + half_ten_pct>("filter-10.csv");
  RunExperiment<half - 2 * half_ten_pct, half + 2 * half_ten_pct>("filter-20.csv");
  RunExperiment<half - 3 * half_ten_pct, half + 3 * half_ten_pct>("filter-30.csv");
  RunExperiment<half - 4 * half_ten_pct, half + 4 * half_ten_pct>("filter-40.csv");
  RunExperiment<half - 5 * half_ten_pct, half + 5 * half_ten_pct>("filter-50.csv");
  RunExperiment<half - 6 * half_ten_pct, half + 6 * half_ten_pct>("filter-60.csv");
  RunExperiment<half - 7 * half_ten_pct, half + 7 * half_ten_pct>("filter-70.csv");
  RunExperiment<half - 8 * half_ten_pct, half + 8 * half_ten_pct>("filter-80.csv");
  RunExperiment<half - 9 * half_ten_pct, half + 9 * half_ten_pct>("filter-90.csv");
}

}  // namespace tpl::sql
