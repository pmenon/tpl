#include "benchmark/benchmark.h"

#include "logging/logger.h"

int main(int argc, char **argv) {
  tpl::logging::InitLogger();

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  tpl::logging::ShutdownLogger();

  return 0;
}
