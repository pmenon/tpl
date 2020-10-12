#include <vector>

#include "benchmark/benchmark.h"
#include "sql/constant_vector.h"
#include "sql/filter_manager.h"
#include "sql/schema.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl {

class SelectionBenchmark : public benchmark::Fixture {
 protected:
  static std::unique_ptr<sql::VectorProjection> MakeInput(uint64_t selectivity) {
    auto int_type = sql::TypeId::Integer;
    auto vec_projection = std::make_unique<sql::VectorProjection>();
    vec_projection->Initialize({int_type, int_type, int_type, int_type});
    vec_projection->Reset(kDefaultVectorSize);

    sql::TupleIdList tids(kDefaultVectorSize);
    std::uniform_int_distribution<uint64_t> dist(0, 100);
    std::mt19937 gen(std::random_device{}());
    for (uint32_t col_idx = 0; col_idx < vec_projection->GetColumnCount(); col_idx++) {
      sql::Vector *vector = vec_projection->GetColumn(col_idx);
      for (uint64_t i = 0; i < vector->GetSize(); i++) {
        reinterpret_cast<int32_t *>(vector->GetData())[i] = dist(gen);
      }
    }

    for (uint64_t i = 0; i < kDefaultVectorSize; i++) {
      if (dist(gen) <= selectivity) {
        tids.Add(i);
      }
    }

    vec_projection->SetFilteredSelections(tids);

    return vec_projection;
  }
};

BENCHMARK_DEFINE_F(SelectionBenchmark, TaaT_Conjunction)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));

  // Copy list so we can restore the VP later.
  auto tid_list = sql::TupleIdList(kDefaultVectorSize);
  vp->CopySelectionsTo(&tid_list);

  for (auto _ : state) {
    sql::VectorProjectionIterator vpi(vp.get());
    for (; vpi.HasNext(); vpi.Advance()) {
      if (*vpi.GetValue<int32_t, false>(0, nullptr) == 0 &&
          *vpi.GetValue<int32_t, false>(1, nullptr) == 0) {
        vpi.Match(true);
      }
    }
    vpi.Reset();
    uint64_t count = vpi.GetSelectedTupleCount();
    benchmark::DoNotOptimize(count);
    vp->SetFilteredSelections(tid_list);
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, TaaT_Disjunction)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));

  // Copy list so we can restore the VP later.
  auto tid_list = sql::TupleIdList(kDefaultVectorSize);
  vp->CopySelectionsTo(&tid_list);

  for (auto _ : state) {
    sql::VectorProjectionIterator vpi(vp.get());
    for (; vpi.HasNext(); vpi.Advance()) {
      if (*vpi.GetValue<int32_t, false>(0, nullptr) == 0 ||
          *vpi.GetValue<int32_t, false>(1, nullptr) == 0) {
        vpi.Match(true);
      }
    }
    vpi.Reset();
    uint64_t count = vpi.GetSelectedTupleCount();
    benchmark::DoNotOptimize(count);
    vp->SetFilteredSelections(tid_list);
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, VaaT_Conjunction)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));

  // Copy list so we can restore the VP later.
  auto tid_list = sql::TupleIdList(kDefaultVectorSize);
  vp->CopySelectionsTo(&tid_list);

  sql::FilterManager filter_manager;
  filter_manager.StartNewClause();
  filter_manager.InsertClauseTerms(
      {[](auto vp, auto tid_list, auto ctx) {
         const auto const_vec = sql::ConstantVector(sql::GenericValue::CreateInteger(0));
         sql::VectorOps::SelectEqual(*vp->GetColumn(0), const_vec, tid_list);
       },
       [](auto vp, auto tid_list, auto ctx) {
         const auto const_vec = sql::ConstantVector(sql::GenericValue::CreateInteger(0));
         sql::VectorOps::SelectEqual(*vp->GetColumn(1), const_vec, tid_list);
       }});

  for (auto _ : state) {
    filter_manager.RunFilters(vp.get());
    uint64_t count = vp->GetSelectedTupleCount();
    benchmark::DoNotOptimize(count);
    vp->SetFilteredSelections(tid_list);
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, VaaT_Disjunction)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));

  // Copy list so we can restore the VP later.
  auto tid_list = sql::TupleIdList(kDefaultVectorSize);
  vp->CopySelectionsTo(&tid_list);

  sql::FilterManager filter_manager;
  filter_manager.StartNewClause();
  filter_manager.InsertClauseTerm([](auto vp, auto tid_list, auto ctx) {
    const auto const_vec = sql::ConstantVector(sql::GenericValue::CreateInteger(0));
    sql::VectorOps::SelectEqual(*vp->GetColumn(0), const_vec, tid_list);
  });
  filter_manager.StartNewClause();
  filter_manager.InsertClauseTerm([](auto vp, auto tid_list, auto ctx) {
    const auto const_vec = sql::ConstantVector(sql::GenericValue::CreateInteger(0));
    sql::VectorOps::SelectEqual(*vp->GetColumn(1), const_vec, tid_list);
  });

  for (auto _ : state) {
    filter_manager.RunFilters(vp.get());
    uint64_t count = vp->GetSelectedTupleCount();
    benchmark::DoNotOptimize(count);
    vp->SetFilteredSelections(tid_list);
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, TaaT_SelectBetween)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    sql::TupleIdList tids(vp->GetTotalTupleCount());
    vp->CopySelectionsTo(&tids);

    auto *RESTRICT in = (int32_t *)vp->GetColumn(0)->GetData();
    tids.Filter([&](auto i) { return in[i] > 20 && in[i] < 50; });

    uint64_t count;
    benchmark::DoNotOptimize(count = tids.GetTupleCount());
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, VaaT_SelectBetween)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    sql::TupleIdList tids(vp->GetTotalTupleCount());
    vp->CopySelectionsTo(&tids);

    const auto lo = sql::ConstantVector(sql::GenericValue::CreateInteger(20));
    const auto hi = sql::ConstantVector(sql::GenericValue::CreateInteger(50));
    sql::VectorOps::SelectBetween(*vp->GetColumn(0), lo, hi, false, false, &tids);

    uint64_t count;
    benchmark::DoNotOptimize(count = tids.GetTupleCount());
  }
}

BENCHMARK_DEFINE_F(SelectionBenchmark, VaaT_SelectBetween_Separate)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    sql::TupleIdList tids(vp->GetTotalTupleCount());
    vp->CopySelectionsTo(&tids);

    const auto lo = sql::ConstantVector(sql::GenericValue::CreateInteger(20));
    const auto hi = sql::ConstantVector(sql::GenericValue::CreateInteger(50));
    sql::VectorOps::SelectGreaterThan(*vp->GetColumn(0), lo, &tids);
    sql::VectorOps::SelectLessThan(*vp->GetColumn(1), hi, &tids);

    uint64_t count;
    benchmark::DoNotOptimize(count = tids.GetTupleCount());
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(SelectionBenchmark, TaaT_Conjunction)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, TaaT_Disjunction)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, VaaT_Conjunction)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, VaaT_Disjunction)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, TaaT_SelectBetween)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, VaaT_SelectBetween)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(SelectionBenchmark, VaaT_SelectBetween_Separate)->DenseRange(0, 100, 10);

}  // namespace tpl
