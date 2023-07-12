#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(PageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    this->Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.page_ = nullptr;
  that.guard_.bpm_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    that.guard_.page_ = nullptr;
    that.guard_.bpm_ = nullptr;
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.page_ = nullptr;
  that.guard_.bpm_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    guard_.bpm_ = that.guard_.bpm_;
    guard_.page_ = that.guard_.page_;
    guard_.is_dirty_ = that.guard_.is_dirty_;
    that.guard_.page_ = nullptr;
    that.guard_.bpm_ = nullptr;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
}  // NOLINT

}  // namespace bustub
