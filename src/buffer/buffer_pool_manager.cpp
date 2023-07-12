//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  std::cout << pool_size_ << "----" << replacer_k << '\n';
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lk(latch_);
  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    page_table_[*page_id] = free_list_.front();
    auto page = &pages_[free_list_.front()];
    page->pin_count_ = 1;
    page->ResetMemory();
    page->page_id_ = *page_id;
    page->is_dirty_ = false;
    replacer_->RecordAccess(free_list_.front());
    free_list_.pop_front();
    return page;
  }
  if (replacer_->Size() != 0) {
    int frame_id;
    replacer_->Evict(&frame_id);
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    page_table_.erase(page_table_.find(pages_[frame_id].page_id_));
    pages_[frame_id].page_id_ = *page_id;
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    if (!free_list_.empty()) {
      page_table_[page_id] = free_list_.front();
      pages_[page_table_[page_id]].pin_count_ = 1;
      pages_[page_table_[page_id]].is_dirty_ = false;
      disk_manager_->ReadPage(page_id, pages_[free_list_.front()].GetData());
      pages_[free_list_.front()].page_id_ = page_id;
      free_list_.pop_front();
      replacer_->RecordAccess(page_table_[page_id]);
      return &pages_[page_table_[page_id]];
    }
    if (replacer_->Size() > 0) {
      int frame_id = -1;
      replacer_->Evict(&frame_id);
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      pages_[frame_id].ResetMemory();
      page_table_.erase(page_table_.find(pages_[frame_id].page_id_));
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].is_dirty_ = false;
      page_table_[page_id] = frame_id;
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      replacer_->Remove(frame_id);
      replacer_->RecordAccess(frame_id);
      return &pages_[frame_id];
    }
    return nullptr;
  }
  if (replacer_->GetEvictable(page_table_[page_id])) {
    replacer_->SetEvictable(page_table_[page_id], false);
  }
  pages_[page_table_[page_id]].pin_count_ += 1;
  replacer_->RecordAccess(page_table_[page_id]);
  return &pages_[page_table_[page_id]];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  if (pages_[page_table_.find(page_id)->second].pin_count_ <= 0) {
    return false;
  }
  pages_[page_table_.find(page_id)->second].pin_count_ -= 1;
  pages_[page_table_.find(page_id)->second].is_dirty_ |= is_dirty;
  if (pages_[page_table_.find(page_id)->second].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_.find(page_id)->second, true);
    // if (pages_[page_table_.find(page_id)->second].IsDirty()) {

    //   pages_[page_table_.find(page_id)->second].is_dirty_ = false;
    // }
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  if (pages_[page_table_.find(page_id)->second].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[page_table_.find(page_id)->second].GetData());
    pages_[page_table_.find(page_id)->second].is_dirty_ = false;
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [pageid, frameid] : page_table_) {
    FlushPage(pageid);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  if (pages_[page_table_.find(page_id)->second].GetPinCount() > 0) {
    return false;
  }
  auto frameid = page_table_.find(page_id)->second;
  page_table_.erase(pages_[frameid].page_id_);
  if (pages_[frameid].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frameid].GetData());
  }
  pages_[frameid].ResetMemory();
  pages_[frameid].pin_count_ = 0;
  pages_[frameid].is_dirty_ = false;
  free_list_.push_back(frameid);
  replacer_->Remove(frameid);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
