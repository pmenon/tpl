#include <memory>
#include <vector>

#include <random>

#include "benchmark/benchmark.h"
#include "sql/tuple_id_list.h"
#include "sql/vector.h"

namespace tpl {

class TupleIdListBenchmark : public benchmark::Fixture {
 protected:
  static std::tuple<std::unique_ptr<sql::Vector>, std::unique_ptr<sql::Vector>,
                    std::unique_ptr<sql::TupleIdList>>
  MakeInput(double sel) {
    auto v1 = std::make_unique<sql::Vector>(sql::TypeId::Integer, true, true);
    auto v2 = std::make_unique<sql::Vector>(sql::TypeId::Integer, true, true);
    v1->Resize(kDefaultVectorSize);
    v2->Resize(kDefaultVectorSize);
    auto tid_list = std::make_unique<sql::TupleIdList>(kDefaultVectorSize);

    int64_t limit = sel * 100.0;
    std::random_device r;
    for (uint32_t i = 0; i < kDefaultVectorSize; i++) {
      reinterpret_cast<int32_t *>(v1->GetData())[i] = r() % 5;
      reinterpret_cast<int32_t *>(v2->GetData())[i] = r() % 5;
      if (r() % 100 < limit) {
        tid_list->Add(i);
      }
    }

    return std::make_tuple(std::move(v1), std::move(v2), std::move(tid_list));
  }
};

BENCHMARK_DEFINE_F(TupleIdListBenchmark, CallbackBasedIteration)(benchmark::State &state) {
  auto [v1, v2, tid] = MakeInput(static_cast<double>(state.range(0)) / 100.0);
  for (auto _ : state) {
    uint64_t count = 0;
    auto v1data = reinterpret_cast<int32_t *>(v1->GetData());
    auto v2data = reinterpret_cast<int32_t *>(v2->GetData());
    tid->Iterate([&](uint64_t i) { count += v1data[i] + v2data[i]; });
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(TupleIdListBenchmark, ConvertToSelectionVectorAndIterate)
(benchmark::State &state) {
  sel_t sel_vector[kDefaultVectorSize];
  auto [v1, v2, tid] = MakeInput(static_cast<double>(state.range(0)) / 100.0);
  for (auto _ : state) {
    uint64_t count = 0;
    auto v1data = reinterpret_cast<int32_t *>(v1->GetData());
    auto v2data = reinterpret_cast<int32_t *>(v2->GetData());

    auto size = tid->ToSelectionVector(sel_vector);
    for (uint32_t i = 0; i < size; i++) count += v1data[sel_vector[i]] + v2data[sel_vector[i]];

    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(TupleIdListBenchmark, IterateSelectionVector)
(benchmark::State &state) {
  sel_t sel_vector[kDefaultVectorSize];
  auto [v1, v2, tid] = MakeInput(static_cast<double>(state.range(0)) / 100.0);
  const auto size = tid->ToSelectionVector(sel_vector);
  for (auto _ : state) {
    uint64_t count = 0;
    auto v1data = reinterpret_cast<int32_t *>(v1->GetData());
    auto v2data = reinterpret_cast<int32_t *>(v2->GetData());

    for (uint32_t i = 0; i < size; i++) count += v1data[sel_vector[i]] + v2data[sel_vector[i]];

    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(TupleIdListBenchmark, ConvertToByteVectorThenSelectionVectorThenIterate)
(benchmark::State &state) {
  uint8_t byte_vector[kDefaultVectorSize];
  sel_t sel_vector[kDefaultVectorSize];

  auto [v1, v2, tid] = MakeInput(static_cast<double>(state.range(0)) / 100.0);
  for (auto _ : state) {
    uint64_t count = 0;
    auto v1data = reinterpret_cast<int32_t *>(v1->GetData());
    auto v2data = reinterpret_cast<int32_t *>(v2->GetData());

    // Bits to byte vector
    util::VectorUtil::BitVectorToByteVector(tid->GetMutableBits()->GetWords(),
                                            tid->GetMutableBits()->GetNumBits(), byte_vector);

    // Byte vector to selection vector
    auto size =
        util::VectorUtil::ByteVectorToSelectionVector(byte_vector, kDefaultVectorSize, sel_vector);

    for (uint32_t i = 0; i < size; i++) count += v1data[sel_vector[i]] + v2data[sel_vector[i]];

    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(TupleIdListBenchmark, ManualIteration)(benchmark::State &state) {
  auto [v1, v2, tid] = MakeInput(static_cast<double>(state.range(0)) / 100.0);
  for (auto _ : state) {
    uint64_t count = 0;
    auto v1data = reinterpret_cast<int32_t *>(v1->GetData());
    auto v2data = reinterpret_cast<int32_t *>(v2->GetData());
    for (const auto i : *tid) count += v1data[i] + v2data[i];
    benchmark::DoNotOptimize(count);
  }
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(TupleIdListBenchmark, CallbackBasedIteration)->DenseRange(0, 100, 10);

BENCHMARK_REGISTER_F(TupleIdListBenchmark, ConvertToSelectionVectorAndIterate)
    ->DenseRange(0, 100, 10);

BENCHMARK_REGISTER_F(TupleIdListBenchmark, IterateSelectionVector)->DenseRange(0, 100, 10);

BENCHMARK_REGISTER_F(TupleIdListBenchmark, ConvertToByteVectorThenSelectionVectorThenIterate)
    ->DenseRange(0, 100, 10);

BENCHMARK_REGISTER_F(TupleIdListBenchmark, ManualIteration)->DenseRange(0, 100, 10);

}  // namespace tpl
