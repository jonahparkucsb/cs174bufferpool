#include "buffer/buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];                                  // Allocating memory for buffer pool
  page_table_ = new std::unordered_map<page_id_t, frame_id_t>();  // Creating page table
  replacer_ = new LRUKReplacer(pool_size, replacer_k);            // Initializing LRU-K replacer

  // Populating the free frame list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;     // Deallocating the buffer pool memory
  delete page_table_;  // Deleting the page table
  delete replacer_;    // Deleting the replacer
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  Page *p = nullptr;
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[frame_id];
  } else if (replacer_->Evict(&frame_id)) {
    p = &pages_[frame_id];
    if (p->IsDirty()) {
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
    }
    page_table_->erase(p->GetPageId());
  } else {
    return nullptr;
  }

  *page_id = AllocatePage();
  p->is_dirty_ = false;
  p->pin_count_ = 1;
  p->page_id_ = *page_id;
  p->ResetMemory();

  page_table_->emplace(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return p;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  Page *p = nullptr;
  frame_id_t frame_id;

  auto it = page_table_->find(page_id);
  if (it != page_table_->end()) {
    frame_id = it->second;
    p = &pages_[frame_id];
    p->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return p;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[frame_id];
  } else if (replacer_->Evict(&frame_id)) {
    p = &pages_[frame_id];

    if (p->IsDirty()) {
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
    }
    page_table_->erase(p->GetPageId());

  } else {
    return nullptr;
  }

  p->is_dirty_ = false;
  p->page_id_ = page_id;
  p->pin_count_ = 1;

  disk_manager_->ReadPage(p->GetPageId(), p->GetData());
  page_table_->emplace(p->GetPageId(), frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return p;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto page_itr = page_table_->find(page_id);

  if (page_itr == page_table_->end()) {
    return false;
  }

  frame_id_t frame_id = page_itr->second;
  Page *unpinned = &pages_[frame_id];

  if (unpinned->GetPinCount() <= 0) {
    return false;
  }

  unpinned->pin_count_--;
  if (is_dirty) {
    unpinned->is_dirty_ = true;
  }

  if (unpinned->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto p_i = page_table_->find(page_id);
  if (p_i == page_table_->end()) {
    return false;
  }

  frame_id_t frame_id = p_i->second;
  Page *p = &pages_[frame_id];
  disk_manager_->WritePage(p->GetPageId(), p->GetData());
  p->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  for (const auto &entry : *page_table_) {
    frame_id_t frame_id = entry.second;
    Page *p = &pages_[frame_id];
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_->find(page_id) == page_table_->end()) {
    return true;
  }

  frame_id_t frame_id = page_table_->at(page_id);
  Page *delete_page = &pages_[frame_id];

  if (delete_page->pin_count_ > 0) {
    return false;
  }

  free_list_.emplace_back(frame_id);
  page_table_->erase(page_id);
  replacer_->Remove(frame_id);
  delete_page->ResetMemory();
  delete_page->is_dirty_ = false;
  delete_page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);

  return true;
}
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
}  // namespace bustub