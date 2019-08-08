#include <chrono>  // NOLINT
#include <thread>  // NOLINT
#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/filter_manager.h"
#include "sql/vector_filter_runner.h"
#include "sql/table_vector_iterator.h"

namespace tpl::sql::test {

class FilterManagerTest : public SqlBasedTest {};

enum Col : u8 { A = 0, B = 1, C = 2, D = 3 };

u32 TaaT_Lt_500(VectorProjectionIterator *vpi) {
  vpi->RunFilter([vpi]() -> bool {
    auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
    return cola < 500;
  });
  return vpi->GetTupleCount();
}

u32 Hobbled_TaaT_Lt_500(VectorProjectionIterator *vpi) {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return TaaT_Lt_500(vpi);
}

u32 Vectorized_Lt_500(VectorProjectionIterator *vpi) {
  VectorFilterRunner filter(vpi);
  filter.SelectLtVal(Col::A, GenericValue::CreateInteger(500));
  filter.Finish();
  return vpi->GetTupleCount();
}

TEST_F(FilterManagerTest, SimpleFilterManagerTest) {
  FilterManager filter(bandit::Policy::FixedAction);
  filter.StartNewClause();
  filter.InsertClauseFlavor(TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }
}

TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  FilterManager filter(bandit::Policy::EpsilonGreedy);
  filter.StartNewClause();
  filter.InsertClauseFlavor(Hobbled_TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }

  // The vectorized filter better be the optimal!
  EXPECT_EQ(1u, filter.GetOptimalFlavorForClause(0));
}

#if 0
template <i32 Choice, i32 Min, i32 Max>
struct Comp;

template <i32 Min, i32 Max>
struct Comp<0, Min, Max> {
  static bool Apply(i32 a, i32 b, i32 c) { return a >= Min; }

  static u32 Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = Min};
    return vpi->FilterColByVal<std::greater_equal>(0, param);
  }
};

template <i32 Min, i32 Max>
struct Comp<1, Min, Max> {
  static bool Apply(i32 a, i32 b, i32 c) { return a < Max; }

  static u32 Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = Max};
    return vpi->FilterColByVal<std::less>(0, param);
  }
};

template <i32 Min, i32 Max>
struct Comp<2, Min, Max> {
  static constexpr i32 val = 2;

  static bool Apply(i32 a, i32 b, i32 c) { return b >= val; }

  static u32 Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::greater_equal>(1, param);
  }
};

template <i32 Min, i32 Max>
struct Comp<3, Min, Max> {
  static constexpr i32 val = 5;

  static bool Apply(i32 a, i32 b, i32 c) { return b < val; }

  static u32 Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::less>(1, param);
  }
};

template <i32 Min, i32 Max>
struct Comp<4, Min, Max> {
  static constexpr i32 val = 6048;

  static bool Apply(i32 a, i32 b, i32 c) { return c <= val; }

  static u32 Apply(VectorProjectionIterator *vpi) {
    VectorProjectionIterator::FilterVal param{.i = val};
    return vpi->FilterColByVal<std::less_equal>(2, param);
  }
};

struct Config {
  template <i32 Min, i32 Max>
  static bool Apply(i32 a, i32 b, i32 c) {
    return true;
  }

  template <i32 Min, i32 Max, i32 First, i32... Next>
  static bool Apply(i32 a, i32 b, i32 c) {
    return Comp<First, Min, Max>::Apply(a, b, c) &&
           Apply<Min, Max, Next...>(a, b, c);
  }

  template <i32 Min, i32 Max>
  static u32 Apply(VectorProjectionIterator *vpi) {
    return 0;
  }

  template <i32 Min, i32 Max, i32 First, i32... Next>
  static u32 Apply(VectorProjectionIterator *vpi) {
    return Comp<First, Min, Max>::Apply(vpi) && Apply<Min, Max, Next...>(vpi);
  }
};

template <i32 Min, i32 Max, u32... Order>
u32 Vector(VectorProjectionIterator *vpi) {
  Config::Apply<Min, Max, Order...>(vpi);
  vpi->ResetFiltered();
  return vpi->num_selected();
}

template <i32 Min, i32 Max, u32... Order>
u32 Scalar(VectorProjectionIterator *vpi) {
  for (; vpi->HasNext(); vpi->Advance()) {
    auto cola = *vpi->GetValue<i32, false>(0, nullptr);
    auto colb = *vpi->GetValue<i32, false>(1, nullptr);
    auto colc = *vpi->GetValue<i32, false>(2, nullptr);
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

template <i32 Min, i32 Max>
void RunExperiment(bandit::Policy::Kind policy_kind, u32 arg,
                   std::vector<double> &results) {
  constexpr u32 num_runs = 3;
  for (u32 run = 0; run < num_runs; run++) {
    FilterManager filter(policy_kind, arg);
    filter.StartNewClause();
    ALL_ORDERS(Scalar)
    ALL_ORDERS(Vector)
    filter.Finalize();

    TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
    for (tvi.Init(); tvi.Advance();) {
      auto *vpi = tvi.vector_projection_iterator();
      filter.RunFilters(vpi);
    }

    if (run == 0) {
      results = filter.timings();
    } else {
      const auto timings = filter.timings();
      for (u32 i = 0; i < timings.size(); i++) {
        results[i] += timings[i];
      }
    }
  }

  for (u32 i = 0; i < results.size(); i++) {
    results[i] /= num_runs;
  }
}

template <i32 Min, i32 Max>
void RunExperiment(std::string output) {
  const u32 num_orders = 0;

  std::vector<double> epsilon, ucb, annealing_epsilon;
  std::vector<std::vector<double>> fixed_order_times(num_orders);

  RunExperiment<Min, Max>(bandit::Policy::UCB, 0, ucb);
  RunExperiment<Min, Max>(bandit::Policy::EpsilonGreedy, 0, epsilon);
  RunExperiment<Min, Max>(bandit::Policy::AnnealingEpsilonGreedy, 0, annealing_epsilon);
#if 0
  for (u32 i = 0; i < num_orders; i++) {
    RunExperiment<Min, Max>(bandit::Policy::FixedAction, i,
                            fixed_order_times[i]);
  }
#endif

  std::ofstream out_file;
  out_file.open(output.c_str());

  out_file << "Partition, ";
  for (u32 order = 0; order < num_orders; order++) {
    out_file << "Order" << order << ", ";
  }
  out_file << "UCB, Epsilon, Annealing Epsilon" << std::endl;

  u32 num_parts = ucb.size();
  for (u32 i = 0; i < num_parts; i++) {
    out_file << i << ", ";
    // First the fixed order timings
    std::cout << std::fixed << std::setprecision(5);
    for (u32 j = 0; j < num_orders; j++) {
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
  static constexpr u32 num_elems = 20000000;
  static constexpr u32 half = num_elems / 2;
  static constexpr u32 ten_pct = num_elems / 10;
  static constexpr u32 half_ten_pct = ten_pct / 2;

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

}  // namespace tpl::sql::test
