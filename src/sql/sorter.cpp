#include "sql/sorter.h"

#include <algorithm>
#include <utility>

#include "ips4o/ips4o.hpp"

#include "logging/logger.h"
#include "util/timer.h"

namespace tpl::sql {

Sorter::Sorter(util::Region *region, ComparisonFunction cmp_fn, u32 tuple_size)
    : tuple_storage_(region, tuple_size),
      cmp_fn_(cmp_fn),
      tuples_(region),
      sorted_(false) {}

Sorter::~Sorter() = default;

byte *Sorter::AllocInputTuple() {
  byte *ret = tuple_storage_.append();
  tuples_.push_back(ret);
  return ret;
}

byte *Sorter::AllocInputTupleTopK(UNUSED u64 top_k) {
  return AllocInputTuple();
}

void Sorter::AllocInputTupleTopKFinish(const u64 top_k) {
  // If the number of buffered tuples is less than top_k, we're done
  if (tuples_.size() < top_k) {
    return;
  }

  // If the number of buffered tuples matches tok_k, let's build the heap. Note,
  // this will only ever be done once!
  if (tuples_.size() == top_k) {
    BuildHeap();
    return;
  }

  //
  // We've buffered one more tuple than should be in the top-K, so we may need
  // to reorder the heap. Check if the most recently inserted tuple belongs in
  // the heap.
  //

  const byte *last_insert = tuples_.back();
  tuples_.pop_back();

  const byte *heap_top = tuples_.front();

  if (cmp_fn_(last_insert, heap_top) <= 0) {
    // The last inserted tuples belongs in the top-k. Swap it with the current
    // maximum and sift it down.
    tuples_.front() = last_insert;
    HeapSiftDown();
  }
}

void Sorter::BuildHeap() {
  const auto compare = [this](const byte *left, const byte *right) {
    return cmp_fn_(left, right) < 0;
  };
  std::make_heap(tuples_.begin(), tuples_.end(), compare);
}

void Sorter::HeapSiftDown() {
  const u64 size = tuples_.size();
  u32 idx = 0;

  const byte *top = tuples_[idx];

  while (true) {
    u32 child = (2 * idx) + 1;

    if (child >= size) {
      break;
    }

    if (child + 1 < size && cmp_fn_(tuples_[child], tuples_[child + 1]) < 0) {
      child++;
    }

    if (cmp_fn_(top, tuples_[child]) >= 0) {
      break;
    }

    std::swap(tuples_[idx], tuples_[child]);
    idx = child;
  }

  tuples_[idx] = top;
}

void Sorter::Sort() {
  // Exit if the input tuples have already been sorted
  if (sorted_) {
    return;
  }

  // Exit if there are no input tuples
  if (tuples_.empty()) {
    return;
  }

  // Time it
  util::Timer<std::milli> timer;
  timer.Start();

  // Sort the sucker
  const auto compare = [this](const byte *left, const byte *right) {
    return cmp_fn_(left, right) < 0;
  };
  ips4o::sort(tuples_.begin(), tuples_.end(), compare);

  timer.Stop();

  UNUSED double tps = (tuples_.size() / timer.elapsed()) / 1000.0;
  LOG_DEBUG("Sorted {} tuples in {} ms ({:.2f} tps)", tuples_.size(),
            timer.elapsed(), tps);

  // Mark complete
  sorted_ = true;
}

}  // namespace tpl::sql
