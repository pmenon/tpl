#include <memory>
#include <mutex>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "logging/logger.h"
#include "sql/aggregation_hash_table.h"
#include "sql/catalog.h"
#include "sql/generic_value.h"
#include "sql/table_vector_iterator.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"
#include "util/fast_rand.h"
#include "util/timer.h"

#define PROFILE 1

namespace tpl {

namespace {

constexpr uint32_t kNumTableRows = 10000000;

struct AggTuple {
  int64_t key, count1, count2, count3;
};

// This array lists component selectivities for each column A, B, C required to
// produced a desired overall selectivity.
constexpr struct TableMeta {
  const char *name;
  uint32_t num_aggregates;
} kAggConfigs[] = {
    {"FM_01", 1U << 0U},   {"FM_02", 1U << 1U},    {"FM_04", 1U << 2U},    {"FM_08", 1U << 3U},
    {"FM_16", 1U << 4U},   {"FM_32", 1U << 5U},    {"FM_64", 1U << 6U},    {"FM_128", 1U << 7U},
    {"FM_256", 1U << 8U},  {"FM_512", 1U << 9U},   {"FM_1K", 1U << 10U},   {"FM_2K", 1U << 11U},
    {"FM_4K", 1U << 12U},  {"FM_8K", 1U << 13U},   {"FM_16K", 1U << 14U},  {"FM_32K", 1U << 15U},
    {"FM_64K", 1U << 16U}, {"FM_128K", 1U << 17U}, {"FM_256K", 1U << 18U}, {"FM_512K", 1U << 19U},
    {"FM_1M", 1U << 20U},
};

constexpr uint32_t kNumAggConfigs = sizeof(kAggConfigs) / sizeof(kAggConfigs[0]);

int32_t *GenColumnData(uint32_t num_vals, uint32_t min, uint32_t max) {
  auto *data =
      static_cast<int32_t *>(Memory::MallocAligned(sizeof(int32_t) * num_vals, CACHELINE_SIZE));
  std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<uint32_t> distribution(min, max);
  for (uint32_t i = 0; i < num_vals; i++) {
    data[i] = distribution(generator);
  }
  return data;
}

// Load the given test table.
void LoadTestTable(const TableMeta &meta, sql::Table *table) {
  const uint32_t batch_size = 10000;
  const uint32_t num_batches = kNumTableRows / batch_size;

  // Insert batches for this phase.
  for (uint32_t i = 0; i < num_batches; i++) {
    std::vector<sql::ColumnSegment> columns;
    auto *keys = reinterpret_cast<byte *>(GenColumnData(batch_size, 0, meta.num_aggregates - 1));
    auto *vals = reinterpret_cast<byte *>(GenColumnData(batch_size, 0, 10));

    columns.emplace_back(sql::IntegerType::Instance(false), keys, nullptr, batch_size);
    columns.emplace_back(sql::IntegerType::Instance(false), vals, nullptr, batch_size);

    // Insert into table
    table->Insert(sql::Table::Block(std::move(columns), batch_size));
  }
}

// Create all test tables. One per overall selectivity.
void InitTestTables() {
  sql::Catalog *catalog = sql::Catalog::Instance();
  for (const auto &table_meta : kAggConfigs) {
    // Create the table instance.
    std::vector<sql::Schema::ColumnInfo> cols = {{"key", sql::IntegerType::Instance(false)},
                                                 {"val", sql::IntegerType::Instance(false)}};
    auto table = std::make_unique<sql::Table>(catalog->AllocateTableId(), table_meta.name,
                                              std::make_unique<sql::Schema>(std::move(cols)));

    // Populate it.
    auto exec_micros = util::Time<std::micro>([&] { LoadTestTable(table_meta, table.get()); });
    LOG_DEBUG("Populated table {} ({} ms)", table_meta.name, exec_micros);

    // Insert into catalog.
    catalog->InsertTable(table_meta.name, std::move(table));
  }
}

}  // namespace

class AggregationBenchmark : public benchmark::Fixture {
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
};

BENCHMARK_DEFINE_F(AggregationBenchmark, Agg_TaaT)(benchmark::State &state) {
  auto &table_meta = kAggConfigs[state.range(0)];
  auto table_id = sql::Catalog::Instance()->LookupTableByName(table_meta.name)->GetId();
  for (auto _ : state) {
    sql::MemoryPool mem(nullptr);
    sql::AggregationHashTable aht(&mem, sizeof(AggTuple));
    sql::TableVectorIterator tvi(static_cast<uint16_t>(table_id));

    if (!tvi.Init()) {
      LOG_ERROR("Could not open iterator on table '{}' (id: {})", table_meta.name, table_id);
      exit(1);
    }

    for (uint32_t vec_idx = 0; tvi.Advance(); vec_idx++) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        const auto probe_key = *vpi->GetValue<uint32_t, false>(0, nullptr);
        const auto hash = util::HashUtil::HashCrc(probe_key);

        auto *agg = reinterpret_cast<AggTuple *>(aht.Lookup(
            // hash
            hash,
            // key equality
            [](const void *a, const void *x) -> bool {
              auto *agg = (AggTuple *)a;
              auto *vpi = (sql::VectorProjectionIterator *)x;
              auto probe_key = *vpi->GetValue<uint32_t, false>(0, nullptr);
              return agg->key == probe_key;
            },
            // probe tuple
            vpi));

        if (agg == nullptr) {
          agg = (AggTuple *)aht.AllocInputTuple(hash);
          agg->key = probe_key;
          agg->count1 = agg->count2 = agg->count3 = 0;
        }
        agg->count1 += 1;
        agg->count2 += 2;
        agg->count3 += 3;
      });
    }
  }
}

namespace {

void InitAggs(sql::VectorProjectionIterator *RESTRICT a,
              sql::VectorProjectionIterator *RESTRICT input) {
  for (; a->HasNext(); a->Advance(), input->Advance()) {
    auto agg = (*a->GetValue<sql::HashTableEntry *, false>(1, nullptr))->PayloadAs<AggTuple>();
    agg->key = *input->GetValue<uint32_t, false>(0, nullptr);
    agg->count1 = agg->count2 = agg->count3 = 0;
  }
}

void UpdateAggs(sql::VectorProjectionIterator *RESTRICT a,
                sql::VectorProjectionIterator *RESTRICT input) {
  for (; a->HasNext(); a->Advance(), input->Advance()) {
    auto agg = (*a->GetValue<sql::HashTableEntry *, false>(1, nullptr))->PayloadAs<AggTuple>();
    agg->count1 += 1;
    agg->count2 += 2;
    agg->count3 += 3;
  }
}

}  // namespace

BENCHMARK_DEFINE_F(AggregationBenchmark, Agg_VaaT)(benchmark::State &state) {
#if PROFILE == 1
  printf("Enter: ");
  int x = getchar();
  (void)x;
#endif

  auto &table_meta = kAggConfigs[state.range(0)];
  auto table = sql::Catalog::Instance()->LookupTableByName(table_meta.name);
  auto table_id = table->GetId();
  for (auto _ : state) {
    sql::MemoryPool mem(nullptr);
    sql::AggregationHashTable aht(&mem, sizeof(AggTuple));
    sql::TableVectorIterator tvi(static_cast<uint16_t>(table_id));

    if (!tvi.Init()) {
      LOG_ERROR("Could not open iterator on table '{}' (id: {})", table_meta.name, table_id);
      exit(1);
    }

    for (uint32_t vec_idx = 0; tvi.Advance(); vec_idx++) {
      aht.ProcessBatch(tvi.GetVectorProjectionIterator(), {0}, InitAggs, UpdateAggs, false);
    }
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(AggregationBenchmark, Agg_TaaT)
    ->DenseRange(0, kNumAggConfigs - 1)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(AggregationBenchmark, Agg_VaaT)
    ->DenseRange(0, kNumAggConfigs - 1)
    ->Unit(benchmark::kMillisecond);

}  // namespace tpl
