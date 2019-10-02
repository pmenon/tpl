#include "util/region.h"

#include <algorithm>
#include <string>
#include <utility>

#include "common/memory.h"

namespace tpl::util {

Region::Region(std::string_view name) noexcept
    : name_(name.data()),
      allocated_(0),
      alignment_waste_(0),
      chunk_bytes_allocated_(0),
      head_(nullptr),
      position_(0),
      end_(0) {}

Region::~Region() { FreeAll(); }  // NOLINT(bugprone-exception-escape)

void *Region::Allocate(std::size_t size, std::size_t alignment) {
  TPL_ASSERT(alignment > 0, "Alignment must be greater than 0");

  std::size_t adjustment = MathUtil::AlignmentAdjustment(position_, alignment);

  allocated_ += size;

  // Do we have enough space in the current chunk?
  if (size + adjustment <= end_ - position_) {
    alignment_waste_ += adjustment;
    uintptr_t aligned_ptr = position_ + adjustment;
    position_ = aligned_ptr + size;
    return reinterpret_cast<void *>(aligned_ptr);
  }

  // The current chunk doesn't have enough room, expand the region with at least
  // 'size' more bytes.
  Expand(size + alignment);

  TPL_ASSERT(position_ < end_, "Region chunk's start position higher than end");

  // The new chunk position may not have the desired alignment, fix that now
  uintptr_t aligned_ptr = MathUtil::AlignAddress(position_, alignment);
  alignment_waste_ += (aligned_ptr - position_);
  position_ = aligned_ptr + size;
  return reinterpret_cast<void *>(aligned_ptr);
}

void Region::FreeAll() {
  Chunk *head = head_;
  while (head != nullptr) {
    Chunk *next = head->next;
    std::free(static_cast<void *>(head));
    head = next;
  }

  // Clean up member variables
  head_ = nullptr;
  allocated_ = 0;
  alignment_waste_ = 0;
  chunk_bytes_allocated_ = 0;
  position_ = 0;
  end_ = 0;
}

uintptr_t Region::Expand(std::size_t requested) {
  static constexpr std::size_t kChunkOverhead = sizeof(Chunk);

  //
  // Each expansion increases the size of the chunk we allocate by 2. But, we
  // bound the maximum chunk allocation size.
  //

  Chunk *head = head_;
  const std::size_t prev_size = (head == nullptr ? 0 : head->size);
  const std::size_t new_size_no_overhead = (requested + (prev_size * 2));
  std::size_t new_size = kChunkOverhead + new_size_no_overhead;

  if (new_size < kMinChunkAllocation) {
    new_size = kMinChunkAllocation;
  } else if (new_size > kMaxChunkAllocation) {
    const std::size_t min_new_size = kChunkOverhead + requested;
    const std::size_t max_alloc = kMaxChunkAllocation;
    new_size = std::max(min_new_size, max_alloc);
  }

  // Allocate a new chunk
  auto *new_chunk = static_cast<Chunk *>(std::malloc(new_size));
  new_chunk->Init(head_, new_size);

  // Link it in
  head_ = new_chunk;
  position_ = new_chunk->Start();
  end_ = new_chunk->End();
  chunk_bytes_allocated_ += new_size;

  return position_;
}

}  // namespace tpl::util
