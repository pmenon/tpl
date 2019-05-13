#pragma once

#include "tbb/enumerable_thread_specific.h"

#include "util/common.h"
#include "util/macros.h"
#include "util/region.h"

namespace tpl::sql {

/**
 * Stores information for one execution of a plan.
 */
class RuntimeContext {
 public:
  /**
   * Constructor.
   */
  explicit RuntimeContext(util::Region *mem_pool);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(RuntimeContext);

  /**
   * Return the memory pool.
   */
  util::Region *GetMemPool();

  /**
   * Access the state for the query.
   */
  byte *GetQueryState();

  /**
   * Access the thread-local state for the query.
   */
  byte *GetThreadLocalState();

  /**
   * Access the thread-local state for the query and reinterpret as the given
   * template type.
   */
  template <typename T>
  T *GetThreadLocalStateAs();

  /**
   * Reset the thread-local states to the given size
   */
  void ResetThreadLocalState(u32 size);

  /**
   * Collect an element at offset @em offset from all thread-local states and
   * store a pointer to them in @em container.
   */
  template <typename T, typename Container>
  void CollectThreadLocalStateElements(u32 offset, Container &container);

 private:
  // A handle to thread-local state
  class TLSHandle {
   public:
    TLSHandle() = default;

    // Create
    TLSHandle(util::Region *mem_pool, u32 size);

    // Destroy
    ~TLSHandle();

    // Access the state raw
    byte *AccessState();

    // Access an element in this state at the given byte offset
    byte *AccessStateElement(u32 offset);

    // Access an element in this state at the given byte offset interpreting it
    // as the given template type
    template <typename T>
    T *AccessStateElementAs(u32 offset) {
      return reinterpret_cast<T *>(AccessStateElement(offset));
    }

   private:
    // The pool to allocate from
    util::Region *mem_pool_;
    // The size of the state
    u32 size_;
    // The state
    byte *state_;
  };

 private:
  // Temporary memory pool for allocations done during execution
  util::Region *mem_pool_;
  // The query state. Opaque because it's defined in generated code.
  byte *query_state_;
  // Container for all states of all thread participating in this execution
  tbb::enumerable_thread_specific<TLSHandle> thread_states_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

template <typename T>
T *RuntimeContext::GetThreadLocalStateAs() {
  return reinterpret_cast<T *>(GetThreadLocalState());
}

template <typename T, typename Container>
void RuntimeContext::CollectThreadLocalStateElements(u32 offset,
                                                     Container &container) {
  for (auto &tl_state : thread_states_) {
    container.push_back(tl_state.AccessStateElementAs<T>(offset));
  }
}

}  // namespace tpl::sql
