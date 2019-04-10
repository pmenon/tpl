#pragma once

#include <iostream>
#include <memory>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "storage/garbage_collector.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"

using namespace terrier;

namespace tpl::sql {
class ExecutionStructures {
 public:
  static ExecutionStructures *Instance();

  storage::BlockStore *GetBlockStore() { return block_store_.get(); }

  storage::RecordBufferSegmentPool *GetBufferPool() {
    return buffer_pool_.get();
  }

  storage::LogManager *GetLogManager() { return log_manager_.get(); }

  transaction::TransactionManager *GetTxnManager() {
    return txn_manager_.get();
  }

  terrier::catalog::Catalog *GetCatalog() { return catalog_.get(); }

 private:
  explicit ExecutionStructures();
  std::unique_ptr<storage::BlockStore> block_store_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<storage::LogManager> log_manager_;
  std::unique_ptr<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<catalog::Catalog> catalog_;
};

}  // namespace tpl::sql
