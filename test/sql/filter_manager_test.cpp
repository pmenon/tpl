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

#if 0
void RunExperiment(bool adapt, const std::vector<FilterManager::MatchFn> &terms,
                   std::vector<double> *results) {
  constexpr uint32_t num_runs = 3;
  for (uint32_t run = 0; run < num_runs; run++) {
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
}

template <int32_t Min, int32_t Max>
void RunExperiment(std::string output) {
  // ALL terms here.
  std::vector<FilterManager::MatchFn> terms = {
      // c <= 6048
      [](auto input, auto tids, auto ctx) {
        static constexpr int32_t kCVal = 6048;
        const auto v = GenericValue::CreateInteger(kCVal);
        VectorFilterExecutor::SelectLessThanEqualVal(input, 2, v, tids);
      },
      // b < 5
      [](auto input, auto tids, auto ctx) {
        static constexpr int32_t kBVal = 5;
        const auto v = GenericValue::CreateInteger(kBVal);
        VectorFilterExecutor::SelectGreaterThanEqualVal(input, 1, v, tids);
      },
      // b >= 2
      [](auto input, auto tids, auto ctx) {
        static constexpr int32_t kBVal = 2;
        const auto v = GenericValue::CreateInteger(kBVal);
        VectorFilterExecutor::SelectGreaterThanEqualVal(input, 1, v, tids);
      },
      // a < MAX
      [](auto input, auto tids, auto ctx) {
        const auto val = GenericValue::CreateInteger(Max);
        VectorFilterExecutor::SelectLessThanVal(input, 0, val, tids);
      },
      // a >= MIN
      [](auto input, auto tids, auto ctx) {
        const auto val = GenericValue::CreateInteger(Min);
        VectorFilterExecutor::SelectGreaterThanEqualVal(input, 0, val, tids);
      },
  };
  std::sort(terms.begin(), terms.end());

  std::vector<std::vector<double>> results;
  std::vector<double> adapt_results;

  // Fixed orderings.
  do {
    std::vector<double> perm;
    perm.reserve(1000);
    RunExperiment(false, terms, &perm);
    results.emplace_back(std::move(perm));
  } while (std::next_permutation(terms.begin(), terms.end()));

  // Adaptive.
  RunExperiment(true, terms, &adapt_results);

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

  // Print CSV of total time per ordering.
  {
    std::vector<std::pair<uint32_t, double>> totals;
    for (uint32_t order = 0; order < num_orders; order++) {
      const auto &order_results = results[order];
      totals.emplace_back(order, std::accumulate(order_results.begin(), order_results.end(), 0.0));
    }
    totals.emplace_back(std::numeric_limits<uint32_t>::max(),
                        std::accumulate(adapt_results.begin(), adapt_results.end(), 0.0));
    std::sort(totals.begin(), totals.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });

    std::ofstream out_file;
    out_file.open(output + "-total.csv");
    out_file << "Order, Total Time" << std::endl;
    for (const auto &total : totals) {
      out_file << total.first << ", " ;
      out_file << std::fixed << std::setprecision(5) << total.second << std::endl;
    }
    out_file << std::endl;
  }
}

TEST_F(FilterManagerTest, Experiment) {
  static constexpr uint32_t num_elems = 20000000;
  static constexpr uint32_t half = num_elems / 2;
  static constexpr uint32_t ten_pct = num_elems / 10;
  static constexpr uint32_t half_ten_pct = ten_pct / 2;

  RunExperiment<half, half>("filter-0.csv");
  RunExperiment<half - half_ten_pct, half - half_ten_pct>("filter-10.csv");
  RunExperiment<half - 2 * half_ten_pct, half + 2 * half_ten_pct>("filter-20.csv");
  RunExperiment<half - 3 * half_ten_pct, half + 3 * half_ten_pct>("filter-30.csv");
  RunExperiment<half - 4 * half_ten_pct, half + 4 * half_ten_pct>("filter-40.csv");
  RunExperiment<half - 5 * half_ten_pct, half + 5 * half_ten_pct>("filter-50.csv");
  RunExperiment<half - 6 * half_ten_pct, half + 6 * half_ten_pct>("filter-60.csv");
  RunExperiment<half - 7 * half_ten_pct, half + 7 * half_ten_pct>("filter-70.csv");
  RunExperiment<half - 8 * half_ten_pct, half + 8 * half_ten_pct>("filter-80.csv");
  RunExperiment<half - 9 * half_ten_pct, half + 9 * half_ten_pct>("filter-90.csv");
}
#endif

}  // namespace tpl::sql
