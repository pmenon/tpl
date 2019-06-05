#include "sql/execution_context.h"

namespace tpl::sql {

ExecutionContext::StringAllocator::StringAllocator() : region_("") {}

ExecutionContext::StringAllocator::~StringAllocator() = default;

char *ExecutionContext::StringAllocator::Allocate(std::size_t size) {
  return reinterpret_cast<char *>(region_.Allocate(size));
}

void ExecutionContext::StringAllocator::Deallocate(UNUSED char *str) {
  // No-op. Bulk de-allocated upon destruction.
}

}  // namespace tpl::sql
