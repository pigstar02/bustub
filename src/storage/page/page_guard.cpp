#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this != &that) {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.Drop();
    bpm_->FlushPage(PageId());
    return;
  }
}

void BasicPageGuard::Drop() { bpm_->UnpinPage(PageId(), is_dirty_); }

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.Drop();
    bpm_->FlushPage(PageId());
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (this != &that) {
    this->Drop();
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    guard_.page_->RLatch();
    guard_.bpm_->FlushPage(PageId());
    return;
  }
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    guard_.page_->RLatch();
    guard_.bpm_->FlushPage(PageId());
  }
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
  guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (this != &that) {
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    that.Drop();
    guard_.bpm_->FlushPage(PageId());
    guard_.page_->WLatch();
    return;
  }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    that.Drop();
    guard_.bpm_->FlushPage(PageId());
    guard_.page_->WLatch();
  }
  return *this;
}

void WritePageGuard::Drop() {
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
  guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
