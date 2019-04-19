//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.h
//
// Identification: src/include/codegen/table.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "compiler/codegen.h"
#include "compiler/scan_callback.h"
#include "type/value.h"

namespace tpl {

namespace storage {
class DataTable;
}  // namespace storage

namespace compiler {

//===----------------------------------------------------------------------===//
// This class the main entry point for any code generation that requires
// operating on physical tables. Ideally, there should only be one instance of
// this for every table in the database. This means some form of catalog should
// exist for such tables. Or, every storage::Table will have an instance of this
// "generator" class.
//===----------------------------------------------------------------------===//
class Table {
 public:
  /// Constructor
  explicit Table(storage::DataTable &table);

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(Table);

  /// Generate code to perform a scan over the given table. The table pointer
  /// is provided as the second argument. The scan consumer (third argument)
  /// should be notified when ready to generate the scan loop body.
  void GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                    llvm::Value *tilegroup_start, llvm::Value *tilegroup_end,
                    uint32_t batch_size, llvm::Value *predicate_array,
                    size_t num_predicates, ScanCallback &consumer) const;

 private:
  // The table associated with this generator
  storage::DataTable &table_;
};

}  // namespace compiler
}  // namespace tpl