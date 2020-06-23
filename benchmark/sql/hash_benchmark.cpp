#include <initializer_list>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "sql/filter_manager.h"
#include "sql/schema.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl {

enum Col : uint8_t { Tiny = 0, Small = 1, Int = 2, Big = 3, ShortString = 4, LongString = 5 };

class HashBenchmark : public benchmark::Fixture {
 protected:
  static constexpr auto kShortStringSizeLimit = sql::VarlenEntry::GetInlineThreshold();
  static constexpr auto kLongStringSizeLimit = 100;

  std::string RandomString(std::size_t length) {
    auto randchar = []() -> char {
      const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
      const size_t max_index = (sizeof(charset) - 1);
      return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
  }

  std::unique_ptr<sql::VectorProjection> MakeInput(uint64_t selectivity) {
    auto vp = std::make_unique<sql::VectorProjection>();
    vp->Initialize({
        sql::TypeId::TinyInt,   // Tiny
        sql::TypeId::SmallInt,  // Small
        sql::TypeId::Integer,   // Int
        sql::TypeId::BigInt,    // Big
        sql::TypeId::Varchar,   // Short
        sql::TypeId::Varchar,   // Long
    });
    vp->Reset(kDefaultVectorSize);

    std::mt19937_64 gen(std::random_device{}());
    std::uniform_int_distribution<uint8_t> tiny_dist(0);
    std::uniform_int_distribution<uint16_t> small_dist(0);
    std::uniform_int_distribution<uint32_t> int_dist(0);
    std::uniform_int_distribution<uint64_t> big_dist(0);

    // String columns. Used later.
    auto short_str_col = vp->GetColumn(Col::ShortString);
    auto long_str_col = vp->GetColumn(Col::LongString);
    auto short_str_heap = short_str_col->GetMutableStringHeap();
    auto long_str_heap = long_str_col->GetMutableStringHeap();

    for (uint64_t i = 0; i < vp->GetTotalTupleCount(); i++) {
      reinterpret_cast<int8_t *>(vp->GetColumn(Col::Tiny)->GetData())[i] = tiny_dist(gen);
      reinterpret_cast<int16_t *>(vp->GetColumn(Col::Small)->GetData())[i] = small_dist(gen);
      reinterpret_cast<int32_t *>(vp->GetColumn(Col::Int)->GetData())[i] = int_dist(gen);
      reinterpret_cast<int64_t *>(vp->GetColumn(Col::Big)->GetData())[i] = big_dist(gen);

      // Strings.
      reinterpret_cast<sql::VarlenEntry *>(short_str_col->GetData())[i] =
          short_str_heap->AddVarlen(RandomString(tiny_dist(gen) % kShortStringSizeLimit));
      reinterpret_cast<sql::VarlenEntry *>(long_str_col->GetData())[i] =
          long_str_heap->AddVarlen(RandomString(tiny_dist(gen) % kLongStringSizeLimit));
    }

    sql::TupleIdList tids(kDefaultVectorSize);
    for (uint64_t i = 0; i < kDefaultVectorSize; i++) {
      if (int_dist(gen) % 100 <= selectivity) {
        tids.Add(i);
      }
    }
    vp->SetFilteredSelections(tids);

    return vp;
  }
};

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_1)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Tiny), &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_2)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Tiny), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Small), &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_3)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Tiny), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Small), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Int), &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_4)(benchmark::State &state) {
  auto vp = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Tiny), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Small), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Int), &hashes);
    sql::VectorOps::Hash(*vp->GetColumn(Col::Big), &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_ShortString)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    vec_projection->Hash({Col::ShortString}, &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, VaaT_Hash_LongString)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    vec_projection->Hash({Col::LongString}, &hashes);
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

#define HASH(x, s) util::HashUtil::HashMurmur(x, s)

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_1)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<int8_t, false>(Col::Tiny, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] = HASH(*col_a, 0);
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_2)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);

    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<int8_t, false>(Col::Tiny, nullptr);
      auto col_b = vpi.GetValue<int16_t, false>(Col::Small, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] = HASH(*col_b, HASH(*col_a, 0));
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_3)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);

    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<int8_t, false>(Col::Tiny, nullptr);
      auto col_b = vpi.GetValue<int16_t, false>(Col::Small, nullptr);
      auto col_c = vpi.GetValue<int32_t, false>(Col::Int, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] = HASH(*col_c, HASH(*col_b, HASH(*col_a, 0)));
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_4)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);

    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<int8_t, false>(Col::Tiny, nullptr);
      auto col_b = vpi.GetValue<int16_t, false>(Col::Small, nullptr);
      auto col_c = vpi.GetValue<int32_t, false>(Col::Int, nullptr);
      auto col_d = vpi.GetValue<int64_t, false>(Col::Big, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] =
          HASH(*col_d, HASH(*col_c, HASH(*col_b, HASH(*col_a, 0))));
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_ShortString)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<sql::VarlenEntry, false>(Col::ShortString, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] = col_a->Hash();
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

BENCHMARK_DEFINE_F(HashBenchmark, TaaT_Hash_LongString)(benchmark::State &state) {
  auto vec_projection = MakeInput(state.range(0));
  auto vpi = sql::VectorProjectionIterator(vec_projection.get());
  for (auto _ : state) {
    auto hashes = sql::Vector(sql::TypeId::Hash, true, false);
    auto raw_hashes = reinterpret_cast<hash_t *>(hashes.GetData());
    for (; vpi.HasNext(); vpi.Advance()) {
      auto col_a = vpi.GetValue<sql::VarlenEntry, false>(Col::LongString, nullptr);
      raw_hashes[vpi.GetCurrentPosition()] = col_a->Hash();
    }
    vpi.Reset();
    benchmark::DoNotOptimize(hashes.GetValue(44).IsNull());
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_1)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_2)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_3)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_4)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_ShortString)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, VaaT_Hash_LongString)->DenseRange(0, 100, 10);

BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_1)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_2)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_3)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_4)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_ShortString)->DenseRange(0, 100, 10);
BENCHMARK_REGISTER_F(HashBenchmark, TaaT_Hash_LongString)->DenseRange(0, 100, 10);

}  // namespace tpl
