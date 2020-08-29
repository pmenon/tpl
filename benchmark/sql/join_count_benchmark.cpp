#include <unordered_set>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/memory.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/data_types.h"
#include "sql/join_hash_table.h"
#include "sql/join_manager.h"
#include "sql/table.h"
#include "sql/table_vector_iterator.h"
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
  std::unique_ptr<sql::JoinHashTable> jht1, jht2, jht3, jht4, jht5;
};

constexpr sql::FilterManager::MatchFn kJoinManagerSteps[] = {
    [](auto input_batch, auto tids, auto opaque_ctx) {
      auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
      query_state->jm->PrepareSingleJoin(input_batch, tids, 0);
    },
    [](auto input_batch, auto tids, auto opaque_ctx) {
      auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
      query_state->jm->PrepareSingleJoin(input_batch, tids, 1);
    },
    [](auto input_batch, auto tids, auto opaque_ctx) {
      auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
      query_state->jm->PrepareSingleJoin(input_batch, tids, 2);
    },
    [](auto input_batch, auto tids, auto opaque_ctx) {
      auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
      query_state->jm->PrepareSingleJoin(input_batch, tids, 3);
    },
    [](auto input_batch, auto tids, auto opaque_ctx) {
      auto *query_state = reinterpret_cast<QueryState *>(opaque_ctx);
      query_state->jm->PrepareSingleJoin(input_batch, tids, 4);
    },
};

struct JoinMeta {
  std::vector<double> sels;
} kJoinConfigs[] = {
    {{0.100, 0.000, 0.000, 0.000, 0.000}},  // 1 join:  0.1
    {{0.750, 0.133, 0.000, 0.000, 0.000}},  // 2 join2: 0.75*0.1333=0.1
    {{0.850, 0.500, 0.230, 0.000, 0.000}},  // 3 joins: 0.85*0.5*0.23=0.1
    {{0.850, 0.750, 0.650, 0.240, 0.000}},  // 4 joins: 0.85*0.75*0.65*0.23=0.1
    {{0.850, 0.750, 0.650, 0.550, 0.438}},  // 5 joins: 0.85*0.75*0.65*0.55*0.438=0.1
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
      columns.emplace_back(sql::IntegerType::Instance(false), col_data, nullptr, batch_size);
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
    cols.emplace_back("col" + std::to_string(i), sql::IntegerType::Instance(false));
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
  JoinManagerBenchmark() : mem_(nullptr) {}

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

 protected:
  uint16_t GetProbeTableId() {
    auto table = sql::Catalog::Instance()->LookupTableByName(kProbeTableName);
    if (table == nullptr) throw std::runtime_error("No table!");
    return table->GetId();
  }

  std::unique_ptr<QueryState> MakeQueryState() {
    auto query_state = std::make_unique<QueryState>();
    query_state->jht1 = std::make_unique<sql::JoinHashTable>(&mem_, sizeof(JoinRow), false);
    query_state->jht2 = std::make_unique<sql::JoinHashTable>(&mem_, sizeof(JoinRow), false);
    query_state->jht3 = std::make_unique<sql::JoinHashTable>(&mem_, sizeof(JoinRow), false);
    query_state->jht4 = std::make_unique<sql::JoinHashTable>(&mem_, sizeof(JoinRow), false);
    query_state->jht5 = std::make_unique<sql::JoinHashTable>(&mem_, sizeof(JoinRow), false);
    query_state->jm = std::make_unique<sql::JoinManager>(query_state.get());
    return query_state;
  }

 private:
  sql::MemoryPool mem_;
};

// Build a join hash table.
void BuildHT(sql::JoinHashTable *jht, double selectivity) {
  if (selectivity != 0) {
    std::unordered_set<uint32_t> selected;
    std::random_device r;
    std::mt19937 gen(r());
    std::uniform_int_distribution dist(0U, kNumTableRows);
    while (static_cast<double>(selected.size()) / static_cast<double>(kNumTableRows) <
           selectivity) {
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
  LOG_DEBUG("JHT built with {} entries", jht->GetTupleCount());
}

void BuildAllJHTs(const JoinMeta &meta, QueryState *state) {
  BuildHT(state->jht1.get(), meta.sels[0]);
  BuildHT(state->jht2.get(), meta.sels[1]);
  BuildHT(state->jht3.get(), meta.sels[2]);
  BuildHT(state->jht4.get(), meta.sels[3]);
  BuildHT(state->jht5.get(), meta.sels[4]);
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder_1)(benchmark::State &state) {
  auto query_state = MakeQueryState();
  BuildAllJHTs(kJoinConfigs[0], query_state.get());

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state->jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            count++;
          }
        }
      });
    }
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder_2)(benchmark::State &state) {
  auto query_state = MakeQueryState();
  BuildAllJHTs(kJoinConfigs[1], query_state.get());

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);

        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state->jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto iter2 = query_state->jht2->Lookup<false>(b_hash_val); iter2.HasNext();) {
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

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder_3)(benchmark::State &state) {
  auto query_state = MakeQueryState();
  BuildAllJHTs(kJoinConfigs[2], query_state.get());

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);
        auto c_val = *vpi->GetValue<int32_t, false>(2, nullptr);

        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state->jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto iter2 = query_state->jht2->Lookup<false>(b_hash_val); iter2.HasNext();) {
              if (auto *jr2 = iter2.GetMatch()->PayloadAs<JoinRow>(); jr2->key == b_val) {
                auto c_hash_val = util::HashUtil::Hash(c_val);
                for (auto iter3 = query_state->jht3->Lookup<false>(c_hash_val); iter3.HasNext();) {
                  if (auto *jr3 = iter3.GetMatch()->PayloadAs<JoinRow>(); jr3->key == c_val) {
                    count++;
                  }
                }
              }
            }
          }
        }
      });
    }
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder_4)(benchmark::State &state) {
  auto query_state = MakeQueryState();
  BuildAllJHTs(kJoinConfigs[3], query_state.get());

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);
        auto c_val = *vpi->GetValue<int32_t, false>(2, nullptr);
        auto d_val = *vpi->GetValue<int32_t, false>(3, nullptr);

        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state->jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto iter2 = query_state->jht2->Lookup<false>(b_hash_val); iter2.HasNext();) {
              if (auto *jr2 = iter2.GetMatch()->PayloadAs<JoinRow>(); jr2->key == b_val) {
                auto c_hash_val = util::HashUtil::Hash(c_val);
                for (auto iter3 = query_state->jht3->Lookup<false>(c_hash_val); iter3.HasNext();) {
                  if (auto *jr3 = iter3.GetMatch()->PayloadAs<JoinRow>(); jr3->key == c_val) {
                    auto d_hash_val = util::HashUtil::Hash(d_val);
                    for (auto iter4 = query_state->jht4->Lookup<false>(d_hash_val);
                         iter4.HasNext();) {
                      if (auto *jr4 = iter4.GetMatch()->PayloadAs<JoinRow>(); jr4->key == d_val) {
                        count++;
                      }
                    }
                  }
                }
              }
            }
          }
        }
      });
    }
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(JoinManagerBenchmark, StaticOrder_5)(benchmark::State &state) {
  auto query_state = MakeQueryState();
  BuildAllJHTs(kJoinConfigs[4], query_state.get());

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      vpi->ForEach([&]() {
        auto a_val = *vpi->GetValue<int32_t, false>(0, nullptr);
        auto b_val = *vpi->GetValue<int32_t, false>(1, nullptr);
        auto c_val = *vpi->GetValue<int32_t, false>(2, nullptr);
        auto d_val = *vpi->GetValue<int32_t, false>(3, nullptr);
        auto e_val = *vpi->GetValue<int32_t, false>(4, nullptr);

        auto a_hash_val = util::HashUtil::Hash(a_val);
        for (auto iter1 = query_state->jht1->Lookup<false>(a_hash_val); iter1.HasNext();) {
          if (auto *jr1 = iter1.GetMatch()->PayloadAs<JoinRow>(); jr1->key == a_val) {
            auto b_hash_val = util::HashUtil::Hash(b_val);
            for (auto iter2 = query_state->jht2->Lookup<false>(b_hash_val); iter2.HasNext();) {
              if (auto *jr2 = iter2.GetMatch()->PayloadAs<JoinRow>(); jr2->key == b_val) {
                auto c_hash_val = util::HashUtil::Hash(c_val);
                for (auto iter3 = query_state->jht3->Lookup<false>(c_hash_val); iter3.HasNext();) {
                  if (auto *jr3 = iter3.GetMatch()->PayloadAs<JoinRow>(); jr3->key == c_val) {
                    auto d_hash_val = util::HashUtil::Hash(d_val);
                    for (auto iter4 = query_state->jht4->Lookup<false>(d_hash_val);
                         iter4.HasNext();) {
                      if (auto *jr4 = iter4.GetMatch()->PayloadAs<JoinRow>(); jr4->key == d_val) {
                        auto e_hash_val = util::HashUtil::Hash(e_val);
                        for (auto iter5 = query_state->jht5->Lookup<false>(e_hash_val);
                             iter5.HasNext();) {
                          if (auto *jr5 = iter5.GetMatch()->PayloadAs<JoinRow>();
                              jr5->key == e_val) {
                            count++;
                          }
                        }
                      }
                    }
                  }
                }
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
  const auto num_joins = state.range(0);

  auto query_state = MakeQueryState();

  // Build JHTs.
  BuildAllJHTs(kJoinConfigs[num_joins - 1], query_state.get());

  // Setup JM.
  {
    const std::vector<sql::JoinHashTable *> tables = {
        query_state->jht1.get(), query_state->jht2.get(), query_state->jht3.get(),
        query_state->jht4.get(), query_state->jht5.get(),
    };
    for (uint32_t i = 0; i < num_joins; i++) {
      query_state->jm->InsertJoinStep(*tables[i], {i}, kJoinManagerSteps[i]);
    }
  }

  for (auto _ : state) {
    uint32_t count = 0;
    sql::TableVectorIterator tvi(GetProbeTableId());
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();

      // Setup batch.
      query_state->jm->SetInputBatch(vpi);

      // Iterate all matches.
      while (query_state->jm->Next()) {
        const sql::HashTableEntry **matches[kNumTableCols];
        query_state->jm->GetOutputBatch(matches);
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

BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder_1)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder_2)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder_3)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder_4)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, StaticOrder_5)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(JoinManagerBenchmark, Adaptive)
    ->DenseRange(1, 5)
    ->Unit(benchmark::kMillisecond);

}  // namespace tpl
