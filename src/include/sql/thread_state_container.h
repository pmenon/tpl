#pragma once

#include <memory>
#include <vector>

#include "util/region.h"

namespace tpl::sql {

/**
 * This class serves as a container for thread-local data required during query
 * execution. Users create an instance of this class and call @em Reset() to
 * configure it to store thread-local structures of an opaque type with a given
 * size. During query execution, users can call
 * @em AccessThreadStateOfCurrentThread() to access the calling thread's state.
 * Thread-local state is constructed lazily upon first access. If an
 * initialization function was provided to @em Reset, it will get invoked once
 * to initialize the state. Finally, when the container is destroyed, or if it
 * is reset, all existing thread-local structures are destroyed. If a
 * destruction function was provided to @em Reset, it is invoked before the
 * memory backing the state has been freed.
 */
class ThreadStateContainer {
 public:
  /**
   * Function used to initialize a thread's local state upon first use
   */
  using InitFn = void (*)(void *);

  /**
   * Function used to destroy a thread's local state if the container is
   * destructed, or if the states are reset.
   */
  using DestroyFn = void (*)(void *);

  /**
   * Construct a container for all thread state using the given allocator
   * @param memory The memory allocator to use to allocate thread states
   */
  explicit ThreadStateContainer(util::Region *memory);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ThreadStateContainer);

  /**
   * Destructor
   */
  ~ThreadStateContainer();

  /**
   * Reset the thread-local states to the given size.
   */
  void Reset(std::size_t state_size, InitFn init_fn, DestroyFn destroy_fn);

  /**
   * Access the calling thread's thread-local state.
   */
  byte *AccessThreadStateOfCurrentThread();

  /**
   * Access the calling thread's thread-local state and interpret it as the
   * templated type.
   */
  template <typename T>
  T *AccessThreadStateOfCurrentThreadAs() {
    return reinterpret_cast<T *>(AccessThreadStateOfCurrentThread());
  }

  /**
   * Collect all thread-local states and store pointers in the output container
   * @em container.
   * @param container The output container to store the results.
   */
  void CollectThreadLocalStates(std::vector<byte *> &container);

  /**
   * Collect an element at offset @em element_offset from all thread-local
   * states in this container and store pointers in the output container.
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state
   */
  void CollectThreadLocalStateElements(std::vector<byte *> &container,
                                       u32 element_offset);

  /**
   * Collect an element at offset @em element_offset from all thread-local
   * states, interpret them as @em T, and store pointers in the output
   * container.
   * NOTE: This is a little inefficient because it will perform two copies: one
   *       into a temporary vector, and a final copy into the output container.
   *       Don't use in performance-critical code.
   * @tparam T The compile-time type to interpret the state element as
   * @param[out] container The output container to store the results.
   * @param element_offset The offset of the element in the thread-local state
   */
  template <typename T>
  void CollectThreadLocalStateElementsAs(std::vector<T *> &container,
                                         u32 element_offset) {
    std::vector<byte *> tmp;
    CollectThreadLocalStates(tmp);
    container.clear();
    container.resize(tmp.size());
    for (u32 idx = 0; idx < tmp.size(); idx++) {
      container[idx] = reinterpret_cast<T *>(tmp[idx]);
    }
  }

 private:
  // Memory allocator
  util::Region *memory_;
  // Size of each thread's state
  std::size_t state_size_;
  // The function to initialize a thread's local state upon first use
  InitFn init_fn_;
  // The function to destroy a thread's local state when no longer needed
  DestroyFn destroy_fn_;

  // PIMPL
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tpl::sql
