#include "util/region.h"

namespace tpl {

Region::Region(std::string name)
    : name_(std::move(name)),
      allocated_(0),
      chunk_bytes_allocated_(0),
      head_(nullptr),
      position_(0),
      end_(0) {}

Region::~Region() {
  Chunk *head = head_;
  while (head != nullptr) {
    Chunk *next = head->next;
    free(head);
    head = next;
  }
}

void *Region::Allocate(std::size_t size) {
  uintptr_t result = position_;

  if (size > end_ - position_) {
    result = Expand();
  }

  position_ += size;

  allocated_ += size;

  return reinterpret_cast<void *>(result);
}

uintptr_t Region::Expand() {
  static constexpr std::size_t kChunkOverhead = sizeof(Chunk);

  // Allocate a new chunk
  std::size_t size = kChunkOverhead + 1024;
  auto *new_chunk = static_cast<Chunk *>(malloc(size));
  new_chunk->Init(head_, size);

  // Link it in
  head_ = new_chunk;
  position_ = new_chunk->start();
  end_ = new_chunk->end();

  return position_;
}

}  // namespace tpl