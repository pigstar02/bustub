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
#include <cstddef>
#include <ctime>
#include <memory>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
LRUKReplacer::~LRUKReplacer() {
  for (auto [id, node] : node_store_) {
    delete node;
    node = nullptr;
  }
  while (!node_more_k_.empty()) {
    node_more_k_.erase(node_more_k_.begin());
  }
  while (!node_less_k_.empty()) {
    node_less_k_.erase(node_less_k_.begin());
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  if (curr_size_ <= 0) {
    return false;
  }

  if (!node_less_k_.empty()) {
    *frame_id = (*node_less_k_.begin())->GetFid();
    node_store_.erase(node_store_.find(*frame_id));
    curr_size_ -= 1;
    delete (*node_less_k_.begin());
    node_less_k_.erase(node_less_k_.begin());
    return true;
  }
  if (!node_more_k_.empty()) {
    *frame_id = (*node_more_k_.begin())->GetFid();
    node_store_.erase(node_store_.find(*frame_id));
    curr_size_ -= 1;
    delete (*node_more_k_.begin());
    node_more_k_.erase(node_more_k_.begin());
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::unique_lock<std::mutex> lk(latch_);
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("frame id error");
    return;
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    auto *newnode = new LRUKNode;
    newnode->SetK(1);
    newnode->SetFid(frame_id);
    newnode->Push(current_timestamp_++);
    node_store_[frame_id] = newnode;
  } else if (node_store_.find(frame_id)->second->GetK() == k_) {
    if (node_store_.find(frame_id)->second->GetEvictable()) {
      node_more_k_.erase(node_store_.find(frame_id)->second);
      node_store_.find(frame_id)->second->Pop();
      node_store_.find(frame_id)->second->Push(current_timestamp_++);
      node_more_k_.insert(node_store_.find(frame_id)->second);
    } else {
      node_store_.find(frame_id)->second->Pop();
      node_store_.find(frame_id)->second->Push(current_timestamp_++);
    }
  } else {
    node_store_.find(frame_id)->second->SetK(1);
    node_store_.find(frame_id)->second->Push(current_timestamp_++);
    if (node_store_.find(frame_id)->second->GetEvictable() && node_store_.find(frame_id)->second->GetK() == k_) {
      node_less_k_.erase(node_store_.find(frame_id)->second);
      node_more_k_.insert(node_store_.find(frame_id)->second);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lk(latch_);
  auto p_node = node_store_.find(frame_id);
  if (p_node == node_store_.end()) {
    return;
  }
  if (p_node->second->GetEvictable()) {
    curr_size_ -= 1;
    if (!set_evictable) {
      if (p_node->second->GetK() == k_) {
        node_more_k_.erase(p_node->second);
      } else {
        node_less_k_.erase(p_node->second);
      }
    }
  } else {
    if (set_evictable) {
      if (p_node->second->GetK() == k_) {
        node_more_k_.insert(p_node->second);
      } else {
        node_less_k_.insert(p_node->second);
      }
    }
  }
  if (set_evictable) {
    curr_size_ += 1;
  }
  p_node->second->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lk(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_.find(frame_id)->second->GetEvictable()) {
    throw Exception("frame id not Evictable");
    return;
  }
  node_less_k_.erase(node_store_[frame_id]);
  node_more_k_.erase(node_store_[frame_id]);
  if (node_store_[frame_id]->GetEvictable()) {
    curr_size_ -= 1;
  }
  delete node_store_[frame_id];
  node_store_.erase(node_store_.find(frame_id));
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lk(latch_);
  return curr_size_;
}

auto LRUKReplacer::GetEvictable(frame_id_t frame_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  return (*node_store_.find(frame_id)->second).GetEvictable();
}

}  // namespace bustub
