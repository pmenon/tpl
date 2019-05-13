#include "sql/runtime_context.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Runtime Context
// ---------------------------------------------------------

RuntimeContext::RuntimeContext(util::Region *mem_pool)
    : mem_pool_(mem_pool), query_state_(nullptr) {}

util::Region *RuntimeContext::GetMemPool() { return mem_pool_; }

byte *RuntimeContext::GetQueryState() { return query_state_; }

byte *RuntimeContext::GetThreadLocalState() {
  return thread_states_.local().AccessState();
}

void RuntimeContext::ResetThreadLocalState(u32 size) {
  thread_states_ = tbb::enumerable_thread_specific<TLSHandle>(mem_pool_, size);
}

// ---------------------------------------------------------
// Thread-Local-State Handle
// ---------------------------------------------------------

RuntimeContext::TLSHandle::TLSHandle(util::Region *mem_pool, u32 size)
    : mem_pool_(mem_pool),
      size_(size),
      state_(static_cast<byte *>(mem_pool_->Allocate(size, CACHELINE_SIZE))) {}

RuntimeContext::TLSHandle::~TLSHandle() {
  mem_pool_->Deallocate(state_, size_);
}

byte *RuntimeContext::TLSHandle::AccessState() { return state_; }

byte *RuntimeContext::TLSHandle::AccessStateElement(u32 offset) {
  TPL_ASSERT(offset < size_, "Out-of-bounds TLS element access");
  return state_ + offset;
}

}  // namespace tpl::sql
