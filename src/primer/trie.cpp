#include "primer/trie.h"
#include <memory>
#include <string_view>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto node = root_;
  for (char ch : key) {
    if (node == nullptr) {
      return nullptr;
    }
    node = node->children_.find(ch) != node->children_.end() ? node->children_.at(ch) : nullptr;
  }
  if (node == nullptr) {
    return nullptr;
  }
  auto val_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (val_node == nullptr) {
    return nullptr;
  }
  return val_node->is_value_node_ ? val_node->value_.get() : nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  auto node = root_;
  std::vector<std::shared_ptr<const TrieNode>> copy_node;
  auto key_size = key.size();
  decltype(key_size) idx = 0;
  while (idx < key_size && node) {
    char c = key[idx++];
    copy_node.push_back(node);
    node = node->children_.find(c) != node->children_.end() ? node->children_.at(c) : nullptr;
  }
  auto leaf_node =
      node ? std::make_shared<const TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)))
           : std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));

  std::shared_ptr<const TrieNode> child_node = leaf_node;
  while (idx < key_size) {
    char c = key[--key_size];
    std::map<char, std::shared_ptr<const TrieNode>> children{{c, child_node}};
    node = std::make_shared<const TrieNode>(children);
    child_node = node;
  }
  // usul node will update in for loop below
  // when key is "",child node will be new root, there is no node to be copied, so we should update node
  node = child_node;
  for (int i = idx - 1; i >= 0; i--) {
    char c = key[i];
    node = std::shared_ptr<const TrieNode>(copy_node[i]->Clone());
    const_cast<TrieNode *>(node.get())->children_[c] = child_node;
    child_node = node;
  }
  return Trie(node);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  auto node = root_;
  std::vector<std::shared_ptr<const TrieNode>> copy_node;
  auto key_size = key.size();
  decltype(key_size) idx = 0;
  while (idx < key_size && node) {
    char c = key[idx++];
    copy_node.push_back(node);
    node = node->children_.find(c) != node->children_.end() ? node->children_.at(c) : nullptr;
  }
  // not exist the key
  if (idx != key_size || node == nullptr || !(node->is_value_node_)) {
    return *this;
  }
  auto leaf_node = node->children_.empty() ? nullptr : std::make_shared<const TrieNode>(node->children_);

  std::shared_ptr<const TrieNode> child_node = leaf_node;
  for (int i = idx - 1; i >= 0; i--) {
    char c = key[i];
    node = std::shared_ptr<const TrieNode>(copy_node[i]->Clone());
    const_cast<TrieNode *>(node.get())->children_[c] = child_node;
    child_node = node;
  }
  return Trie(node);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
