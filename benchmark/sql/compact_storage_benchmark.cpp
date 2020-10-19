#include "benchmark/benchmark.h"

#include <random>

#include "sql/compact_storage.h"
#include "sql/sorter.h"
#include "sql/value.h"

namespace tpl {

namespace {

constexpr uint32_t kNumElems = 2000000;

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

// 17 + 16 + alignment padding = 40 bytes fat.
// One cache line fat and compressed.
struct A {
  sql::StringVal aa;
  sql::Real bb;

  template <typename NumGen, typename NullGen>
  static A Make(NumGen num, NullGen null) {
    return A{
        sql::StringVal(sql::VarlenEntry::Create(RandomString(10))),         // aa
        null() ? sql::Real::Null() : sql::Real(static_cast<double>(num()))  // bb
    };
  }

  static std::unique_ptr<sql::CompactStorage> MakeStorage() {
    std::vector<sql::TypeId> types = {sql::TypeId::Varchar, sql::TypeId::Double};
    return std::make_unique<sql::CompactStorage>(types);
  }

  template <bool Nullable>
  void Serialize(const sql::CompactStorage &storage, byte *x) const {
    storage.Write<sql::VarlenEntry, Nullable>(0, x, aa.val, aa.is_null);
    storage.Write<double, Nullable>(1, x, bb.val, bb.is_null);
  }
};

// 16 + 16 + 17 + alignment padding = 56 bytes fat.
// One cache line fat and compressed.
struct B {
  sql::Integer aa;
  sql::Integer bb;
  sql::StringVal cc;

  template <typename NumGen, typename NullGen>
  static B Make(NumGen num, NullGen null) {
    return B{
        null() ? sql::Integer::Null() : sql::Integer(num()),         // aa
        null() ? sql::Integer::Null() : sql::Integer(num()),         // bb
        sql::StringVal(sql::VarlenEntry::Create(RandomString(10))),  // cc
    };
  }

  static std::unique_ptr<sql::CompactStorage> MakeStorage() {
    std::vector<sql::TypeId> types = {sql::TypeId::BigInt, sql::TypeId::BigInt,
                                      sql::TypeId::Varchar};
    return std::make_unique<sql::CompactStorage>(types);
  }

  template <bool Nullable>
  void Serialize(const sql::CompactStorage &storage, byte *x) const {
    storage.Write<int64_t, Nullable>(0, x, aa.val, aa.is_null);
    storage.Write<int64_t, Nullable>(1, x, bb.val, bb.is_null);
    storage.Write<sql::VarlenEntry, Nullable>(2, x, cc.val, cc.is_null);
  }
};

// 16 + 16 + 17 + 16 + alignment padding = 72 bytes fat.
// Two cache-lines fat, one cache-line compressed.
struct C {
  sql::Integer aa;
  sql::Integer bb;
  sql::StringVal cc;
  sql::Real dd;

  template <typename NumGen, typename NullGen>
  static C Make(NumGen num, NullGen null) {
    return C{
        null() ? sql::Integer::Null() : sql::Integer(num()),                // aa
        null() ? sql::Integer::Null() : sql::Integer(num()),                // bb
        sql::StringVal(sql::VarlenEntry::Create(RandomString(10))),         // cc
        null() ? sql::Real::Null() : sql::Real(static_cast<double>(num()))  // dd
    };
  }

  static std::unique_ptr<sql::CompactStorage> MakeStorage() {
    std::vector<sql::TypeId> types = {sql::TypeId::BigInt, sql::TypeId::BigInt,
                                      sql::TypeId::Varchar, sql::TypeId::Double};
    return std::make_unique<sql::CompactStorage>(types);
  }

  template <bool Nullable>
  void Serialize(const sql::CompactStorage &storage, byte *x) const {
    storage.Write<int64_t, Nullable>(0, x, aa.val, aa.is_null);
    storage.Write<int64_t, Nullable>(1, x, bb.val, bb.is_null);
    storage.Write<sql::VarlenEntry, Nullable>(2, x, cc.val, cc.is_null);
    storage.Write<double, Nullable>(3, x, dd.val, dd.is_null);
  }
};

// 16 x 8 = 128 bytes fat.
// Two cache-lines fat and compressed.
struct D {
  static constexpr uint32_t kNumElems = 8;

  std::array<sql::Integer, kNumElems> aa{
      sql::Integer::Null(), sql::Integer::Null(), sql::Integer::Null(), sql::Integer::Null(),
      sql::Integer::Null(), sql::Integer::Null(), sql::Integer::Null(), sql::Integer::Null(),
  };

  template <typename NumGen, typename NullGen>
  static D Make(NumGen num, NullGen null) {
    D d;
    for (auto &x : d.aa) {
      x = null() ? sql::Integer::Null() : sql::Integer(num());
    }
    return d;
  }

  static std::unique_ptr<sql::CompactStorage> MakeStorage() {
    std::vector<sql::TypeId> types(kNumElems);
    for (uint32_t i = 0; i < kNumElems; i++) types[i] = sql::TypeId::BigInt;
    return std::make_unique<sql::CompactStorage>(types);
  }

  template <bool Nullable>
  void Serialize(const sql::CompactStorage &storage, byte *x) const {
    for (uint32_t i = 0; i < aa.size(); i++) {
      storage.Write<int64_t, Nullable>(i, x, aa[i].val, aa[i].is_null);
    }
  }
};

template <typename T>
std::vector<T> MakeInput() {
  std::vector<T> input;
  input.reserve(kNumElems);

  std::uniform_int_distribution<uint32_t> dist(0, 100);
  std::mt19937 gen(std::random_device{}());

  const auto number_gen = [&]() { return dist(gen); };
  const auto null_gen = [&]() { return dist(gen) < 50; };  // 50% nulls.

  for (uint32_t i = 0; i < kNumElems; i++) {
    input.emplace_back(T::Make(number_gen, null_gen));
  }

  return input;
}

}  // namespace

template <typename T>
static void BM_Vanilla(benchmark::State &state) {
  auto data = MakeInput<T>();
  for (auto _ : state) {
    sql::MemoryPool mem(nullptr);
    sql::Sorter sorter(&mem, nullptr, sizeof(T));
    for (const T &input : data) {
      *reinterpret_cast<T *>(sorter.AllocInputTuple()) = input;
    }
  }
}

template <typename T, bool Nullable>
static void BM_CompactStorage(benchmark::State &state) {
  auto data = MakeInput<T>();
  for (auto _ : state) {
    auto storage = T::MakeStorage();
    sql::MemoryPool mem(nullptr);
    sql::Sorter sorter(&mem, nullptr, storage->GetRequiredSize());
    for (const auto &input : data) {
      input.template Serialize<Nullable>(*storage, sorter.AllocInputTuple());
    }
  }
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

// A
BENCHMARK_TEMPLATE(BM_Vanilla, A)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, A, false)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, A, true)->Unit(benchmark::kMillisecond);

// B
BENCHMARK_TEMPLATE(BM_Vanilla, B)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, B, false)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, B, true)->Unit(benchmark::kMillisecond);

// C
BENCHMARK_TEMPLATE(BM_Vanilla, C)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, C, false)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, C, true)->Unit(benchmark::kMillisecond);

// D
BENCHMARK_TEMPLATE(BM_Vanilla, D)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, D, false)->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_CompactStorage, D, true)->Unit(benchmark::kMillisecond);

}  // namespace tpl
