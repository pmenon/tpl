#include "util/region.h"

namespace tpl::util {

Region::Region(std::string name)
    : name_(std::move(name)),
      allocated_(0),
      chunk_bytes_allocated_(0),
      head_(nullptr),
      position_(0),
      end_(0) {}

Region::~Region() { FreeAll(); }

void *Region::Allocate(size_t size) {
  size = SizeWithAlignment(size);

  uintptr_t result = position_;

  if (size > end_ - position_) {
    result = Expand(size);
  }

  TPL_ASSERT(position_ < end_);

  position_ += size;

  allocated_ += size;

  return reinterpret_cast<void *>(result);
}

void Region::FreeAll() {
  Chunk *head = head_;
  while (head != nullptr) {
    Chunk *next = head->next;
    free(head);
    head = next;
  }

  // Clean up member variables
  head_ = nullptr;
  allocated_ = 0;
  chunk_bytes_allocated_ = 0;
  position_ = 0;
  end_ = 0;
}

uintptr_t Region::Expand(size_t requested) {
  static constexpr size_t kChunkOverhead = sizeof(Chunk);

  /*
   * Each expansion increases the size of the chunk we allocate by 2. But, we
   * bound the maximum chunk allocation size.
   */

  Chunk *head = head_;
  const size_t prev_size = (head == nullptr ? 0 : head->size);
  const size_t new_size_no_overhead = (requested + (prev_size * 2));
  size_t new_size = kChunkOverhead + new_size_no_overhead;

  if (new_size < kMinChunkAllocation) {
    new_size = kMinChunkAllocation;
  } else if (new_size > kMaxChunkAllocation) {
    const size_t min_new_size = kChunkOverhead + requested;
    const size_t max_alloc = kMaxChunkAllocation;
    new_size = std::max(min_new_size, max_alloc);
  }

  // Allocate a new chunk
  auto *new_chunk = static_cast<Chunk *>(malloc(new_size));
  new_chunk->Init(head_, new_size);

  // Link it in
  head_ = new_chunk;
  position_ = new_chunk->start();
  end_ = new_chunk->end();
  chunk_bytes_allocated_ += new_size;

  return position_;
}

}  // namespace tpl::util