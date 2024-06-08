//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { curr_size_ = 0; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  size_t max_d = 0;
  size_t t = std::numeric_limits<size_t>::max();
  frame_id_t s = INVALID_FRAME_ID;

  for (const auto &[id, info] : frame_table_) {
    if (!info.evictable) {
      continue;
    }

    size_t d;
    if (info.timestamp.size() >= k_) {
      d = current_timestamp_ - *std::next(info.timestamp.rbegin(), k_ - 1);
    } else {
      d = std::numeric_limits<size_t>::max();
    }

    if (((info.timestamp.front() < t) && d == max_d) || d > max_d) {
      s = id;
      t = info.timestamp.front();
      max_d = d;
    }
  }

  if (s == INVALID_FRAME_ID) {
    return false;
  }

  curr_size_--;
  frame_table_.erase(s);
  *frame_id = s;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto &i = frame_table_[frame_id];
  i.timestamp.push_back(current_timestamp_++);

  if (i.timestamp.size() > k_) {
    i.timestamp.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto &i = frame_table_[frame_id];

  if (i.evictable != set_evictable) {
    i.evictable = set_evictable;
    (set_evictable) ? ++curr_size_ : --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid id");

  auto fid = frame_table_.find(frame_id);
  if (fid != frame_table_.end()) {
    --curr_size_;
    BUSTUB_ASSERT(fid->second.evictable, "Can't remove a non-evictable frame");
    frame_table_.erase(fid);
  }
}
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub