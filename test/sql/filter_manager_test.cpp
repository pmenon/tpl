#include <chrono>
#include <string>
#include <vector>

#include "sql/catalog.h"
#include "sql/filter_manager.h"
#include "sql/table_vector_iterator.h"
#include "sql/vector_filter_executor.h"
#include "util/sql_test_harness.h"

namespace tpl::sql {

class FilterManagerTest : public SqlBasedTest {};

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

using namespace std::chrono_literals;  // NOLINT

// 0.025% selective
void ColA_Lt_500(VectorProjection *vp, TupleIdList *tids) {
  VectorFilterExecutor::SelectLessThanVal(vp, Col::A, GenericValue::CreateInteger(500), tids);
}

// 90% selective
void ColB_Lt_9(VectorProjection *vp, TupleIdList *tids) {
  VectorFilterExecutor::SelectLessThanVal(vp, Col::B, GenericValue::CreateInteger(9), tids);
}

// 90% selective tuple-at-a-time filter
void ColB_Lt_9_TaaT(VectorProjection *vp, TupleIdList *tids) {
  VectorProjectionIterator iter(vp, tids);
  iter.RunFilter([&]() {
    bool null = false;
    auto col_b = *iter.GetValue<int32_t, false>(Col::B, &null);
    return col_b < 9;
  });
}

TEST_F(FilterManagerTest, ConjunctionTest) {
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms({ColA_Lt_500, ColB_Lt_9});
  filter.Finalize();

  TableVectorIterator tvi(static_cast<uint16_t>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.GetVectorProjectionIterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<int32_t, false>(Col::A, nullptr);
      auto colb = *vpi->GetValue<int32_t, false>(Col::B, nullptr);
      EXPECT_LT(cola, 500);
      EXPECT_LT(colb, 9);
    });
  }
}

TEST_F(FilterManagerTest, DisjunctionTest) {
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerm(ColA_Lt_500);
  filter.StartNewClause();
  filter.InsertClauseTerm(ColB_Lt_9);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<uint16_t>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.GetVectorProjectionIterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<int32_t, false>(Col::A, nullptr);
      auto colb = *vpi->GetValue<int32_t, false>(Col::B, nullptr);
      EXPECT_TRUE(cola < 500 || colb < 9);
    });
  }
}

TEST_F(FilterManagerTest, MixedTaatVaatFilterTest) {
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms({ColB_Lt_9_TaaT, ColA_Lt_500});
  filter.Finalize();

  TableVectorIterator tvi(static_cast<uint16_t>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.GetVectorProjectionIterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<int32_t, false>(Col::A, nullptr);
      auto colb = *vpi->GetValue<int32_t, false>(Col::B, nullptr);
      EXPECT_LT(cola, 500);
      EXPECT_LT(colb, 9);
    });
  }
}

#if 0
template <int32_t Choice, int32_t Min, int32_t Max>
struct Comp;

template <int32_t Min, int32_t Max>
struct Comp<0, Min, Max> {
  static bool Apply(int32_t a, int32_t b, int32_t c) { return a >= Min; }

  static uint32_t Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = Min};
    return vpi->FilterColByVal<std::greater_equal>(0, param);
  }
};

template <int32_t Min, int32_t Max>
struct Comp<1, Min, Max> {
  static bool Apply(int32_t a, int32_t b, int32_t c) { return a < Max; }

  static uint32_t Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = Max};
    return vpi->FilterColByVal<std::less>(0, param);
  }
};

template <int32_t Min, int32_t Max>
struct Comp<2, Min, Max> {
  static constexpr int32_t val = 2;

  static bool Apply(int32_t a, int32_t b, int32_t c) { return b >= val; }

  static uint32_t Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::greater_equal>(1, param);
  }
};

template <int32_t Min, int32_t Max>
struct Comp<3, Min, Max> {
  static constexpr int32_t val = 5;

  static bool Apply(int32_t a, int32_t b, int32_t c) { return b < val; }

  static uint32_t Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::less>(1, param);
  }
};

template <int32_t Min, int32_t Max>
struct Comp<4, Min, Max> {
  static constexpr int32_t val = 6048;

  static bool Apply(int32_t a, int32_t b, int32_t c) { return c <= val; }

  static uint32_t Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::less_equal>(2, param);
  }
};

struct Config {
  template <int32_t Min, int32_t Max>
  static bool Apply(int32_t a, int32_t b, int32_t c) {
    return true;
  }

  template <int32_t Min, int32_t Max, int32_t First, int32_t... Next>
  static bool Apply(int32_t a, int32_t b, int32_t c) {
    return Comp<First, Min, Max>::Apply(a, b, c) &&
           Apply<Min, Max, Next...>(a, b, c);
  }

  template <int32_t Min, int32_t Max>
  static uint32_t Apply(VectorProjectionIterator *vpi) {
    return 0;
  }

  template <int32_t Min, int32_t Max, int32_t First, int32_t... Next>
  static uint32_t Apply(VectorProjectionIterator *vpi) {
    return Comp<First, Min, Max>::Apply(vpi) && Apply<Min, Max, Next...>(vpi);
  }
};

template <int32_t Min, int32_t Max, uint32_t... Order>
uint32_t Vector(VectorProjectionIterator *vpi) {
  Config::Apply<Min, Max, Order...>(vpi);
  vpi->ResetFiltered();
  return vpi->num_selected();
}

template <int32_t Min, int32_t Max, uint32_t... Order>
uint32_t Scalar(VectorProjectionIterator *vpi) {
  for (; vpi->HasNext(); vpi->Advance()) {
    auto cola = *vpi->GetValue<int32_t, false>(0, nullptr);
    auto colb = *vpi->GetValue<int32_t, false>(1, nullptr);
    auto colc = *vpi->GetValue<int32_t, false>(2, nullptr);
    if (Config::Apply<Min, Max, Order...>(cola, colb, colc)) {
      vpi->Match(true);
    }
  }
  vpi->ResetFiltered();
  return vpi->num_selected();
}

#define ALL_ORDERS(TYPE)                                    \
  filter.InsertClauseFlavor(TYPE<Min, Max, 0, 1, 2, 3, 4>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 2, 1, 4, 3, 0>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 2, 4, 3, 1, 0>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 3, 2, 1, 0, 4>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 3, 4, 0, 1, 2>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 4, 0, 1, 3, 2>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 4, 1, 0, 3, 2>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 4, 3, 1, 2, 0>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 4, 3, 2, 0, 1>); \
  filter.InsertClauseFlavor(TYPE<Min, Max, 4, 3, 2, 1, 0>);

template <int32_t Min, int32_t Max>
void RunExperiment(bandit::Policy::Kind policy_kind, uint32_t arg,
                   std::vector<double> &results) {
  constexpr uint32_t num_runs = 3;
  for (uint32_t run = 0; run < num_runs; run++) {
    FilterManager filter(policy_kind, arg);
    filter.StartNewClause();
    ALL_ORDERS(Scalar)
    ALL_ORDERS(Vector)
    filter.Finalize();

    TableVectorIterator tvi(static_cast<uint16_t>(TableId::Test1));
    for (tvi.Init(); tvi.Advance();) {
      auto *vpi = tvi.vector_projection_iterator();
      filter.RunFilters(vpi);
    }

    if (run == 0) {
      results = filter.timings();
    } else {
      const auto timings = filter.timings();
      for (uint32_t i = 0; i < timings.size(); i++) {
        results[i] += timings[i];
      }
    }
  }

  for (uint32_t i = 0; i < results.size(); i++) {
    results[i] /= num_runs;
  }
}

template <int32_t Min, int32_t Max>
void RunExperiment(std::string output) {
  const uint32_t num_orders = 0;

  std::vector<double> epsilon, ucb, annealing_epsilon;
  std::vector<std::vector<double>> fixed_order_times(num_orders);

  RunExperiment<Min, Max>(bandit::Policy::UCB, 0, ucb);
  RunExperiment<Min, Max>(bandit::Policy::EpsilonGreedy, 0, epsilon);
  RunExperiment<Min, Max>(bandit::Policy::AnnealingEpsilonGreedy, 0, annealing_epsilon);
#if 0
  for (uint32_t i = 0; i < num_orders; i++) {
    RunExperiment<Min, Max>(bandit::Policy::FixedAction, i,
                            fixed_order_times[i]);
  }
#endif

  std::ofstream out_file;
  out_file.open(output.c_str());

  out_file << "Partition, ";
  for (uint32_t order = 0; order < num_orders; order++) {
    out_file << "Order" << order << ", ";
  }
  out_file << "UCB, Epsilon, Annealing Epsilon" << std::endl;

  uint32_t num_parts = ucb.size();
  for (uint32_t i = 0; i < num_parts; i++) {
    out_file << i << ", ";
    // First the fixed order timings
    std::cout << std::fixed << std::setprecision(5);
    for (uint32_t j = 0; j < num_orders; j++) {
      out_file << fixed_order_times[j][i] << ", ";
    }
    // UCB
    out_file << ucb[i] << ", ";
    // Epsilon
    out_file << epsilon[i] << ", ";
    out_file << annealing_epsilon[i] << std::endl;
  }
}

#undef ALL_ORDERS

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
