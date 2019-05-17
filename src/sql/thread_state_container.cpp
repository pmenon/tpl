#include "sql/thread_state_container.h"

#include <memory>
#include <vector>

#include "tbb/tbb.h"

namespace tpl::sql {

/**
 * A handle to a single thread's state
 */
class TLSHandle {
 public:
  TLSHandle() : init_fn_(nullptr), destroy_fn_(nullptr), state_(nullptr) {}

  TLSHandle(std::size_t size, ThreadStateContainer::InitFn init_fn,
            ThreadStateContainer::DestroyFn destroy_fn)
      : init_fn_(init_fn),
        destroy_fn_(destroy_fn),
        state_(std::make_unique<byte[]>(size)) {
    if (init_fn_ != nullptr) {
      init_fn_(state_.get());
    }
  }

  ~TLSHandle() {
    if (destroy_fn_ != nullptr) {
      destroy_fn_(state_.get());
    }
  }

  byte *state() { return state_.get(); }

 private:
  ThreadStateContainer::InitFn init_fn_;
  ThreadStateContainer::DestroyFn destroy_fn_;
  std::unique_ptr<byte[]> state_;
};

/**
 * The actual container for all thread-local state for participating threads
 */
struct ThreadStateContainer::Impl {
  tbb::enumerable_thread_specific<std::unique_ptr<TLSHandle>> states_;
};

ThreadStateContainer::ThreadStateContainer(util::Region *memory)
    : memory_(memory),
      state_size_(0),
      init_fn_(nullptr),
      destroy_fn_(nullptr),
      impl_(std::make_unique<ThreadStateContainer::Impl>()) {}

ThreadStateContainer::~ThreadStateContainer() = default;

void ThreadStateContainer::Reset(const std::size_t state_size, InitFn init_fn,
                                 DestroyFn destroy_fn) {
  state_size_ = state_size;
  init_fn_ = init_fn;
  destroy_fn_ = destroy_fn;
  impl_->states_.clear();
}

byte *ThreadStateContainer::AccessThreadStateOfCurrentThread() {
  bool exists = false;
  auto &tls_handle = impl_->states_.local(exists);
  if (!exists) {
    tls_handle =
        std::make_unique<TLSHandle>(state_size_, init_fn_, destroy_fn_);
  }
  return tls_handle->state();
}

void ThreadStateContainer::CollectThreadLocalStates(
    std::vector<byte *> &container) {
  container.clear();
  container.reserve(impl_->states_.size());
  for (auto &tls_handle : impl_->states_) {
    container.push_back(tls_handle->state());
  }
}

}  // namespace tpl::sql
