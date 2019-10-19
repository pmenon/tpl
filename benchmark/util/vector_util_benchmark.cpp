#include <memory>
#include <random>

#include "benchmark/benchmark.h"
#include "util/bit_vector.h"

namespace tpl::util {

class VectorUtilBenchmark : public benchmark::Fixture {
 protected:
  static BitVector<> CreateBitVectorWithDensity(double density, uint32_t size) {
    BitVector<> bv(size);

    int64_t limit = density * 100.0;
    std::random_device r;
    for (uint32_t i = 0; i < size; i++) {
      if (r() % 100 < limit) {
        bv[i] = true;
      }
    }

    return bv;
  }
};

BENCHMARK_DEFINE_F(VectorUtilBenchmark, BitsToSelectionIndex_Sparse)
(benchmark::State &state) {
  const double density = static_cast<double>(state.range(0)) / 100.0;

  auto bv = CreateBitVectorWithDensity(density, kDefaultVectorSize);
  auto sel_vec = std::array<sel_t, kDefaultVectorSize>();

  for (auto _ : state) {
    const auto size =
        VectorUtil::BitVectorToSelectionVector_Sparse(bv.GetWords(), bv.GetNumBits(), sel_vec.data());
    benchmark::DoNotOptimize(size);
  }
}

BENCHMARK_DEFINE_F(VectorUtilBenchmark, BitsToSelectionIndex_Dense)
(benchmark::State &state) {
  const double density = static_cast<float>(state.range(0)) / 100.0;

  auto bv = CreateBitVectorWithDensity(density, kDefaultVectorSize);
  auto sel_vec = std::array<sel_t, kDefaultVectorSize>();

  for (auto _ : state) {
    const auto size = VectorUtil::BitVectorToSelectionVector_Dense(bv.GetWords(), bv.GetNumBits(),
                                                                   sel_vec.data());
    benchmark::DoNotOptimize(size);
  }
}

BENCHMARK_DEFINE_F(VectorUtilBenchmark, BitsToSelectionIndex_Adaptive)
(benchmark::State &state) {
  const double density = static_cast<float>(state.range(0)) / 100.0;

  auto bv = CreateBitVectorWithDensity(density, kDefaultVectorSize);
  auto sel_vec = std::array<sel_t, kDefaultVectorSize>();

  for (auto _ : state) {
    const auto size =
        VectorUtil::BitVectorToSelectionVector(bv.GetWords(), bv.GetNumBits(), sel_vec.data());
    benchmark::DoNotOptimize(size);
  }
}

BENCHMARK_REGISTER_F(VectorUtilBenchmark, BitsToSelectionIndex_Sparse)->DenseRange(0, 100, 5);
BENCHMARK_REGISTER_F(VectorUtilBenchmark, BitsToSelectionIndex_Dense)->DenseRange(0, 100, 5);
BENCHMARK_REGISTER_F(VectorUtilBenchmark, BitsToSelectionIndex_Adaptive)->DenseRange(0, 100, 5);

}  // namespace tpl::util
