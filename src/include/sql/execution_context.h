#pragma once

#include <memory>
#include <vector>

#include "util/common.h"
#include "util/macros.h"
#include "util/region.h"

namespace tpl::sql {

/**
 * Stores information for one execution of a plan.
 */
class ExecutionContext {
 public:
  class ThreadStateContainer;

  /**
   * Constructor.
   */
  ExecutionContext(util::Region *mem_pool, u32 query_state_size);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ExecutionContext);

  /**
   * Destructor
   */
  ~ExecutionContext();

  /**
   * Return the memory pool.
   */
  util::Region *GetMemPool();

  /**
   * Access the state for the query.
   */
  byte *GetQueryState() { return query_state_; }

  /**
   * Access the calling thread's thread-local state.
   */
  byte *GetThreadLocalState();

  /**
   * Access the thread-local state for the query and reinterpret as the given
   * template type.
   */
  template <typename T>
  T *GetThreadLocalStateAs() {
    return reinterpret_cast<T *>(GetThreadLocalState());
  }

  /**
   * Reset the thread-local states to the given size
   */
  void ResetThreadLocalState(std::size_t size);

  /**
   * Collect all thread-local states and store pointers in the output container
   * @em container
   * @param container The output container to store the results.
   */
  void CollectThreadLocalStates(std::vector<byte *> &container);

  /**
   * Collect an element at offset @em offset from all thread-local states and
   * store a pointer to them in @em container.
   * @param element_offset The offset of the element in the thread-local state
   *                       of interest.
   * @param container The output container to store the results.
   */
  void CollectThreadLocalStateElements(std::size_t element_offset,
                                       std::vector<byte *> &container);

 private:
  // Temporary memory pool for allocations done during execution
  util::Region *mem_pool_;
  // The query state. Opaque because it's defined in generated code.
  byte *query_state_;
  // Handle to all thread states
  std::unique_ptr<ThreadStateContainer> thread_states_;
};

}  // namespace tpl::sql
