#include "benchmark/benchmark.h"

#include "common/cpu_info.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "vm/llvm_engine.h"

int main(int argc, char **argv) {
  tpl::logging::InitLogger();

  benchmark::Initialize(&argc, argv);
  
  // TODO(pmenon): Pull all initialization/shutdown logic into single function.
  tpl::CpuInfo::Instance();
  tpl::sql::Catalog::Instance();
  tpl::vm::LLVMEngine::Initialize();

  benchmark::RunSpecifiedBenchmarks();

  tpl::vm::LLVMEngine::Shutdown();
  tpl::logging::ShutdownLogger();
  return 0;
}
