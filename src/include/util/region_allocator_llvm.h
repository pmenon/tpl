#pragma once

#include "llvm/Support/Allocator.h"

#include "util/region.h"

namespace tpl::util {

/**
 * A region allocator that complies with LLVM's allocator concept.
 */
class LLVMRegionAllocator : public llvm::AllocatorBase<LLVMRegionAllocator> {
 public:
  /**
   * Instantiate an allocator using the provided region.
   * @param region The region to allocate from.
   */
  explicit LLVMRegionAllocator(Region *region) noexcept : region_(region) {}

  /**
   * Allocate memory of the given size and alignment.
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment, in bytes.
   * @return A pointer to the aligned memory of the given size.
   */
  void *Allocate(size_t size, size_t alignment) { return region_->Allocate(size, alignment); }

  /** Pull in base class overloads. */
  using AllocatorBase<LLVMRegionAllocator>::Allocate;

  /**
   * De-allocate memory pointed to by the provided pointer and of the given size and alignment.
   * @param ptr Pointer to the memory to de-allocate.
   * @param size The size (in bytes) of the memory region to de-allocate.
   * @param alignment The alignment (in bytes) of the memory region to de-allocate.
   */
  void Deallocate(const void *ptr, size_t size, UNUSED size_t alignment) {
    region_->Deallocate(ptr, size);
  }

  /** Pull in base class overloads. */
  using AllocatorBase<LLVMRegionAllocator>::Deallocate;

 private:
  // The region where memory is sourced from.
  Region *region_;
};

}  // namespace tpl::util
