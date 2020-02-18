#include "benchmark/benchmark.h"

#include "common/cpu_info.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/tablegen/table_generator.h"
#include "vm/llvm_engine.h"

int main(int argc, char **argv) {
  tpl::logging::InitLogger();

  benchmark::Initialize(&argc, argv);
  tpl::CpuInfo::Instance();
  tpl::sql::Catalog::Instance();
  tpl::sql::tablegen::TableGenerator::GenerateTPCHTables(tpl::sql::Catalog::Instance(),
                                                         "../../tpl_tables/tables/");
  tpl::vm::LLVMEngine::Initialize();

  benchmark::RunSpecifiedBenchmarks();

  tpl::vm::LLVMEngine::Shutdown();
  tpl::logging::ShutdownLogger();
  return 0;
}