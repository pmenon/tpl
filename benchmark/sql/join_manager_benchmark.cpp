#include <unordered_set>
#include <vector>

#include "benchmark/benchmark.h"
#include "sql/catalog.h"
#include "sql/constant_vector.h"
#include "sql/join_hash_table.h"
#include "sql/join_manager.h"
#include "sql/memory_pool.h"
#include "sql/table_vector_iterator.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"

////////////////////////////////////////////////////////////////////////////////
///
/// This requires Test1.A AND Test1.B to be serial [0,200000].
/// By default, Test1.B is uniform random in [0,9].
/// Change to:
///   {"colB", sql::IntegerType::Instance(false), Dist::Serial, 0L, 0L},
///
/// Two JHTs are build. The first has all values in the range [0,2000000] as
/// join keys. The second JHT has [0,N] where N achieves a desired selectivity.
/// That selectivity is driven by the benchmark configuration. To achieve this
/// selectivity, keys are selected RANDOMLY without repetition from the range
/// and inserted into the JHT until N*S keys unique keys have been selected.
///
////////////////////////////////////////////////////////////////////////////////

namespace tpl {

struct JoinRow {
  int32_t key;
  int32_t val;
};

struct QueryState {
  std::unique_ptr<sql::JoinManager> jm;
  std::unique_ptr<sql::JoinHashTable> jht1;
  std::unique_ptr<sql::JoinHashTable> jht2;
};

class JoinManagerBenchmark : public benchmark::Fixture {
 public:
  static uint64_t JHTSize() {
    return sql::Catalog::Instance()
        ->LookupTableById(static_cast<uint16_t>(sql::TableId::Test1))
        ->GetTupleCount();
  }
};

// Build a join hash table.
void BuildHT(sql::JoinHashTable *jht, std::pair<uint32_t, uint32_t> range, double selectivity) {
  if (selectivity != 0) {
    std::unordered_set<uint32_t> selected;
    std::random_device r;
    std::mt19937 gen(r());
    std::uniform_int_distribution dist(range.first, range.second);
    const auto range_size = range.second - range.first;
    while (static_cast<double>(selected.size()) / static_cast<double>(range_size) < selectivity) {
      // Choose unselected key.
      uint32_t key;
      do {
        key = dist(gen);
      } while (selected.count(key) != 0);
      selected.insert(key);

      // Insert into JHT.
      auto hash_val = util::HashUtil::Hash(key);
      auto join_row = reinterpret_cast<JoinRow *>(jht->AllocInputTuple(hash_val));
      join_row->key = key;
      join_row->val = r();
    }
  }
  jht->Build();
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder)(benchmark::State &state) {
  sql::MemoryPool mem_pool(nullptr);
  QueryState query_state;
  query_state.jht1 = std::make_unique<sql::JoinHashTable>(&mem_pool, sizeof(JoinRow), false);
  query_state.jht2 = std::make_unique<sql::JoinHashTable>(&mem_pool, sizeof(JoinRow), false);

  // Build table.
  BuildHT(query_state.jht1.get(), {0, JHTSize()}, 1.0);
  BuildHT(query_state.jht2.get(), {0, JHTSize()}, static_cast<double>(state.range(0)) / 100.0);

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(static_cast<uint16_t>(sql::TableId::Test1));
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);
        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state.jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto iter2 = query_state.jht2->Lookup<false>(b_hash_val); iter2.HasNext();) {
              if (auto *jr2 = iter2.GetMatch()->PayloadAs<JoinRow>(); jr2->key == b_val) {
                count++;
              }
            }
          }
        }
      });
    }
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, Adaptive)(benchmark::State &state) {
  const auto join1_fn = [](auto input_batch, auto tids, auto opaque_ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
    query_state->jm->PrepareSingleJoin(input_batch, tids, 0);
  };
  const auto join2_fn = [](auto input_batch, auto tids, auto opaque_ctx) {
    auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
    query_state->jm->PrepareSingleJoin(input_batch, tids, 1);
  };

  sql::MemoryPool mem_pool(nullptr);
  QueryState query_state;
  query_state.jht1 = std::make_unique<sql::JoinHashTable>(&mem_pool, sizeof(JoinRow), false);
  query_state.jht2 = std::make_unique<sql::JoinHashTable>(&mem_pool, sizeof(JoinRow), false);
  query_state.jm = std::make_unique<sql::JoinManager>(&query_state);
  query_state.jm->InsertJoinStep(*query_state.jht1, {0}, join1_fn);
  query_state.jm->InsertJoinStep(*query_state.jht2, {1}, join2_fn);

  // Build table.
  BuildHT(query_state.jht1.get(), {0, JHTSize()}, 1.0);
  BuildHT(query_state.jht2.get(), {0, JHTSize()}, static_cast<double>(state.range(0)) / 100.0);

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(static_cast<uint16_t>(sql::TableId::Test1));
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      query_state.jm->SetInputBatch(vpi);
      const sql::HashTableEntry **matches[2];
      while (query_state.jm->Next()) {
        query_state.jm->GetOutputBatch(matches);
        count += vpi->GetSelectedTupleCount();
      }
    }
    benchmark::DoNotOptimize(count);
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder)
    ->DenseRange(0, 100, 10)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, Adaptive)
    ->DenseRange(0, 100, 10)
    ->Unit(benchmark::kMillisecond);

}  // namespace tpl
