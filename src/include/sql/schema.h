#pragma once

#include <string>
#include <vector>

#include "sql/data_types.h"

namespace tpl::sql {

/// A class to capture the physical schema layout
class Schema {
 public:
  struct ColInfo {
    std::string name;
    const Type &type;
  };

  explicit Schema(std::vector<ColInfo> cols) : cols_(std::move(cols)) {}

  const std::vector<ColInfo> columns() const { return cols_; }

 private:
  std::vector<ColInfo> cols_;
};

}  // namespace tpl::sql