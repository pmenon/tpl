#include <unordered_set>
#include <vector>

#include "benchmark/benchmark.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/constant_vector.h"
#include "sql/join_hash_table.h"
#include "sql/join_manager.h"
#include "sql/memory_pool.h"
#include "sql/table_vector_iterator.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "util/timer.h"

namespace tpl {

namespace {

constexpr uint32_t kNumTableRows = 2000000;
constexpr uint32_t kNumTableCols = 6;
constexpr const char *kProbeTableName = "JM_Table";

struct JoinRow {
  int32_t key;
  int32_t val;
};

struct QueryState {
  std::unique_ptr<sql::JoinManager> jm;
  std::unique_ptr<sql::JoinHashTable> jht1;
  std::unique_ptr<sql::JoinHashTable> jht2;
};

int32_t *GenColumnData(uint32_t min_val, uint32_t num_vals) {
  auto *data =
      static_cast<int32_t *>(Memory::MallocAligned(sizeof(int32_t) * num_vals, CACHELINE_SIZE));
  std::iota(data, data + num_vals, min_val);
  std::shuffle(data, data + num_vals, std::random_device());
  return data;
}

// Load the given test table.
void LoadTestTable(sql::Table *table) {
  const uint32_t batch_size = 10000;
  const uint32_t num_batches = kNumTableRows / batch_size;

  // Insert batches for this phase.
  for (uint32_t i = 0; i < num_batches; i++) {
    std::vector<sql::ColumnSegment> columns;
    for (uint32_t j = 0; j < kNumTableCols; j++) {
      auto *col_data = reinterpret_cast<byte *>(GenColumnData(i * batch_size, batch_size));
      columns.emplace_back(sql::Type::IntegerType(false), col_data, nullptr, batch_size);
    }

    // Insert into table
    table->Insert(sql::Table::Block(std::move(columns), batch_size));
  }
}

// Create all test tables. One per overall selectivity.
void InitTestTables() {
  sql::Catalog *catalog = sql::Catalog::Instance();

  // Create the table schema.
  std::vector<sql::Schema::ColumnInfo> cols;
  for (uint32_t i = 0; i < kNumTableCols; i++) {
    cols.emplace_back("col" + std::to_string(i), sql::Type::IntegerType(false));
  }

  // Create the table instance.
  auto table = std::make_unique<sql::Table>(catalog->AllocateTableId(), kProbeTableName,
                                            std::make_unique<sql::Schema>(std::move(cols)));

  // Populate it.
  auto exec_millis = util::Time<std::milli>([&] { LoadTestTable(table.get()); });
  LOG_INFO("Populated table {} ({} ms)", table->GetName(), exec_millis);

  // Insert into catalog.
  catalog->InsertTable(table->GetName(), std::move(table));
}

}  // namespace

class JoinManagerBenchmark : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &st) override {
    // Let base setup.
    Fixture::SetUp(st);
    // Now us.
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
      // Create tables.
      InitTestTables();
    });
  }

  uint16_t GetProbeTableId() {
    auto table = sql::Catalog::Instance()->LookupTableByName(kProbeTableName);
    if (table == nullptr) throw std::runtime_error("No table!");
    return table->GetId();
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
  BuildHT(query_state.jht1.get(), {0U, kNumTableRows}, 1.0);
  BuildHT(query_state.jht2.get(), {0U, kNumTableRows}, static_cast<double>(state.range(0)) / 100.0);

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);
        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto e1 = query_state.jht1->Lookup<false>(a_hash_val); e1; e1 = e1->next) {
          if (auto *jr1 = e1->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto e2 = query_state.jht2->Lookup<false>(b_hash_val); e2; e2 = e2->next) {
              if (auto *jr2 = e2->PayloadAs<JoinRow>(); jr2->key == b_val) {
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
  BuildHT(query_state.jht1.get(), {0U, kNumTableRows}, 1.0);
  BuildHT(query_state.jht2.get(), {0U, kNumTableRows}, static_cast<double>(state.range(0)) / 100.0);

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
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
