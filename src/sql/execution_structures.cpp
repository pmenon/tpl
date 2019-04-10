#include "sql/execution_structures.h"
#include <iostream>
#include <memory>

namespace tpl::sql {
ExecutionStructures::ExecutionStructures() {
  block_store_ = std::make_unique<storage::BlockStore>(1000, 1000);
  buffer_pool_ =
      std::make_unique<storage::RecordBufferSegmentPool>(100000, 100000);
  log_manager_ =
      std::make_unique<storage::LogManager>("log_file.log", buffer_pool_.get());
  txn_manager_ = std::make_unique<transaction::TransactionManager>(
      buffer_pool_.get(), true, log_manager_.get());
  catalog_ = std::make_unique<catalog::Catalog>();
}

ExecutionStructures *ExecutionStructures::Instance() {
  static ExecutionStructures kInstance{};
  return &kInstance;
}
}  // namespace tpl::sql
