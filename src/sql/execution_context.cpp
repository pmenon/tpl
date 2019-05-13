#include "sql/execution_context.h"

#include <memory>
#include <vector>

#include "tbb/enumerable_thread_specific.h"

#include "logging/logger.h"

namespace tpl::sql {

// A handle to one thread's thread-local state
class TLSHandle {
 public:
  // No-arg constructor needed for enumerable_thread_specific
  TLSHandle() : size_(0), state_(nullptr) {}

  // Create
  explicit TLSHandle(const std::size_t size)
      : size_(size), state_(std::make_unique<byte[]>(size)) {}

  // Access the state raw
  byte *GetState() { return state_.get(); }

  // Access an element in this state at the given byte offset
  byte *GetStateElement(const std::size_t offset) {
    TPL_ASSERT(offset < size_, "Out-of-bounds TLS element access");
    return &state_[offset];
  }

  // Access an element in this state at the given byte offset interpreting it
  // as the given template type
  template <typename T>
  T *AccessStateElementAs(const std::size_t offset) {
    return reinterpret_cast<T *>(GetStateElement(offset));
  }

 private:
  // The size of the state
  std::size_t size_;
  // The state
  std::unique_ptr<byte[]> state_;
};

// A container to track **all** thread-local states for **all** threads
// participating in execution.
class ExecutionContext::ThreadStateContainer {
 public:
  // Access the calling thread's local state
  byte *AccessThreadLocalState() { return thread_states_.local().GetState(); }

  // Collect all thread states into the output vector
  void CollectThreadLocalStates(std::vector<byte *> &out) {
    CollectThreadLocalStates(0, out);
  }

  // Collect the element at a given offset from all thread states into the
  // output vector.
  void CollectThreadLocalStates(const std::size_t element_offset,
                                std::vector<byte *> &out) {
    for (auto &tls_handle : thread_states_) {
      byte *const tls_elem = tls_handle.GetStateElement(element_offset);
      out.push_back(tls_elem);
    }
  }

  // Reset the thread state to a new size. This will destroy all the old state,
  // if any, and create a new, lazily created thread-local state.
  void Reset(u32 state_size) {
    thread_states_ = tbb::enumerable_thread_specific<TLSHandle>(state_size);
  }

 private:
  tbb::enumerable_thread_specific<TLSHandle> thread_states_;
};

// ---------------------------------------------------------
// Runtime Context
// ---------------------------------------------------------

ExecutionContext::ExecutionContext(util::Region *mem_pool, u32 query_state_size)
    : mem_pool_(mem_pool),
      query_state_(mem_pool_->AllocateArray<byte>(query_state_size)),
      thread_states_(std::make_unique<ThreadStateContainer>()) {}

// Needed because we forward declare ThreadStateContainer
ExecutionContext::~ExecutionContext() = default;

util::Region *ExecutionContext::GetMemPool() { return mem_pool_; }

byte *ExecutionContext::GetThreadLocalState() {
  return thread_states_->AccessThreadLocalState();
}

void ExecutionContext::ResetThreadLocalState(u32 size) {
  thread_states_->Reset(size);
}

void ExecutionContext::CollectThreadLocalStates(
    std::vector<byte *> &container) {
  thread_states_->CollectThreadLocalStates(container);
}

void ExecutionContext::CollectThreadLocalStateElements(
    const std::size_t element_offset, std::vector<byte *> &container) {
  thread_states_->CollectThreadLocalStates(element_offset, container);
}

}  // namespace tpl::sql
