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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    replacer_size_ = 0;
    if (curr_size_ <= 0) {
        return false;
    }
    if (!node_less_k_.empty()) {
        *frame_id = (*node_less_k_.begin())->GetFid();
        node_store_.erase(node_store_.find(*frame_id));
        if ((*node_less_k_.begin())->GetEvictable()) {
            curr_size_ -= 1;
        }
        delete (*node_less_k_.begin());
        node_less_k_.erase(node_less_k_.begin());
        return true;
    }
    if (!node_more_k_.empty()) {
        *frame_id = (*node_more_k_.begin())->GetFid();
        node_store_.erase(node_store_.find(*frame_id));
        if ((*node_more_k_.begin())->GetEvictable()) {
            curr_size_ -= 1;
        }
        delete (*node_more_k_.begin());
        node_more_k_.erase(node_more_k_.begin());
        return true;
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    if (node_store_.find(frame_id) == node_store_.end()) {
        auto *newnode = new LRUKNode;
        newnode->SetK(1);
        newnode->SetFid(frame_id);
        newnode->Push(current_timestamp_ ++);
        node_store_[frame_id] = newnode;
    } else if (node_store_.find(frame_id)->second->GetK() == k_) {
        node_store_.find(frame_id)->second->Pop();
        node_store_.find(frame_id)->second->Push(current_timestamp_ ++);
        if (node_store_.find(frame_id)->second->GetEvictable()) {
            node_more_k_.erase(node_store_.find(frame_id)->second);
            node_more_k_.insert(node_store_.find(frame_id)->second);
        }
    } else {
        node_store_.find(frame_id)->second->SetK(1);
        node_store_.find(frame_id)->second->Push(current_timestamp_ ++);
        if (node_store_.find(frame_id)->second->GetEvictable() &&
            node_store_.find(frame_id)->second->GetK() == k_) {
            node_less_k_.erase(node_store_.find(frame_id)->second);
            node_more_k_.insert(node_store_.find(frame_id)->second);
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    auto p_node = node_store_.find(frame_id);
    if (p_node == node_store_.end()) {
        return;
    }
    if (p_node->second->GetEvictable()) {
        curr_size_ -= 1;
        if (!set_evictable) {
            if (p_node->second->GetK() == k_) {
                node_more_k_.erase(p_node->second);
            }
            else {
                node_less_k_.erase(p_node->second);
            }
        }
    }
    else {
        if (set_evictable) {
            if (p_node->second->GetK() == k_) {
                node_more_k_.insert(p_node->second);
            }
            else {
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
   
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
