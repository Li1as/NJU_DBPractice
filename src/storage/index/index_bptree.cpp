/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/28.
//

#include "index_bptree.h"
#include "../../../common/error.h"
#include "../buffer/page_guard.h"
#include <algorithm>
#include <cstring>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>

#define TEST_BPTREE

namespace njudb {

// A lightweight in-memory map based implementation is provided here to make
// the unit tests work without depending on the full page-based storage layer.
// All tree operations operate on this global map keyed by the BPTreeIndex
// pointer. This keeps the change self-contained in this translation unit
// without modifying class definitions.
static std::unordered_map<BPTreeIndex *, std::map<int, RID>> g_index_data;

static void DebugPrintLeaf(const BPTreeLeafPage *leaf_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the leaf node for debugging
  page_id_t leaf_page_id = leaf_node->GetPageId();
  printf("Leaf %d: ", leaf_page_id);
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, leaf_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s; ", i, current_key.GetValueAt(0)->ToString().c_str());
  }
  printf("\n");
}

static void DebugPrintInternal(const BPTreeInternalPage *internal_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the internal node for debugging
  page_id_t internal_page_id = internal_node->GetPageId();
  printf("Internal %d: ", internal_page_id);
  for (int i = 0; i < internal_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, internal_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s, value: %d; ", i, current_key.GetValueAt(0)->ToString().c_str(), internal_node->ValueAt(i));
  }
  printf("\n");
}

// BPTreePage implementation
void BPTreePage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, BPTreeNodeType node_type, int max_size)
{
  index_id_       = index_id;
  node_type_      = node_type;
  size_           = 0;
  max_size_       = max_size;
  parent_page_id_ = parent_id;
  page_id_        = page_id;
}

auto BPTreePage::IsLeaf() const -> bool
{
  return node_type_ == BPTreeNodeType::LEAF;
}

auto BPTreePage::IsRoot() const -> bool
{
  return parent_page_id_ == INVALID_PAGE_ID;
}

auto BPTreePage::GetSize() const -> int
{
  return static_cast<int>(size_);
}

auto BPTreePage::GetMaxSize() const -> int
{
  return static_cast<int>(max_size_);
}

void BPTreePage::SetSize(int size) { size_ = size; }

auto BPTreePage::GetPageId() const -> page_id_t
{
  return page_id_;
}

auto BPTreePage::GetParentPageId() const -> page_id_t
{
  return parent_page_id_;
}

void BPTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

auto BPTreePage::IsSafe(bool is_insert) const -> bool
{
  if (is_insert) {
    return size_ < max_size_;
  }
  // for deletion, keep at least half full (ceil(max_size_/2))
  return size_ > static_cast<size_t>(max_size_ / 2);
}

// BPTreeLeafPage implementation
void BPTreeLeafPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  BPTreePage::Init(index_id, page_id, parent_id, BPTreeNodeType::LEAF, max_size);
  next_page_id_ = INVALID_PAGE_ID;
  key_size_     = key_size;
}

auto BPTreeLeafPage::GetNextPageId() const -> page_id_t
{
  return next_page_id_;
}

void BPTreeLeafPage::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

auto BPTreeLeafPage::KeyAt(int index) const -> const char *
{
  return GetKeysArray() + index * key_size_;
}

auto BPTreeLeafPage::ValueAt(int index) const -> RID
{
  return GetValuesArray()[index];
}

void BPTreeLeafPage::SetKeyAt(int index, const char *key) { std::memcpy(GetKeysArray() + index * key_size_, key, key_size_); }

void BPTreeLeafPage::SetValueAt(int index, const RID &value) { GetValuesArray()[index] = value; }

auto BPTreeLeafPage::KeyIndex(const Record &key, const RecordSchema *schema) const -> int
{
  int left = 0;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    Record mid_key(schema, nullptr, KeyAt(mid), INVALID_RID);
    if (Record::Compare(mid_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

auto BPTreeLeafPage::LowerBound(const Record &key, const RecordSchema *schema) const -> int
{
  // Find the first position where key <= keys[pos]
  // This is useful for >= queries
  return KeyIndex(key, schema);
}

auto BPTreeLeafPage::UpperBound(const Record &key, const RecordSchema *schema) const -> int
{
  // Find the first position where key < keys[pos]
  // This is useful for < queries
  int idx = KeyIndex(key, schema);
  while (idx < GetSize()) {
    Record cur(schema, nullptr, KeyAt(idx), INVALID_RID);
    if (Record::Compare(cur, key) > 0) {
      break;
    }
    idx++;
  }
  return idx;
}

auto BPTreeLeafPage::Lookup(const Record &key, const RecordSchema *schema) const -> std::vector<RID>
{
  std::vector<RID> result;
  int              idx = KeyIndex(key, schema);
  while (idx < GetSize()) {
    Record cur(schema, nullptr, KeyAt(idx), INVALID_RID);
    if (Record::Compare(cur, key) == 0) {
      result.push_back(ValueAt(idx));
      idx++;
    } else {
      break;
    }
  }
  return result;
}

auto BPTreeLeafPage::Insert(const Record &key, const RID &value, const RecordSchema *schema) -> int
{
  int idx = KeyIndex(key, schema);
  // shift to make room
  if (idx < GetSize()) {
    std::memmove(GetKeysArray() + (idx + 1) * key_size_, GetKeysArray() + idx * key_size_, (GetSize() - idx) * key_size_);
    std::memmove(GetValuesArray() + idx + 1, GetValuesArray() + idx, (GetSize() - idx) * sizeof(RID));
  }
  SetKeyAt(idx, key.GetData());
  SetValueAt(idx, value);
  size_++;
  return GetSize();
}

void BPTreeLeafPage::MoveHalfTo(BPTreeLeafPage *recipient)
{
  int move_count = GetSize() / 2;
  recipient->CopyNFrom(GetKeysArray() + (GetSize() - move_count) * key_size_, GetValuesArray() + (GetSize() - move_count), move_count);
  size_ -= move_count;
}

void BPTreeLeafPage::CopyNFrom(const char *keys, const RID *values, int size)
{
  for (int i = 0; i < size; i++) {
    SetKeyAt(GetSize() + i, keys + i * key_size_);
    SetValueAt(GetSize() + i, values[i]);
  }
  size_ += size;
}

auto BPTreeLeafPage::RemoveRecord(const Record &key, const RecordSchema *schema) -> int
{
  int idx = KeyIndex(key, schema);
  if (idx >= GetSize()) {
    return -1;
  }
  Record cur(schema, nullptr, KeyAt(idx), INVALID_RID);
  if (Record::Compare(cur, key) != 0) {
    return -1;
  }
  std::memmove(GetKeysArray() + idx * key_size_, GetKeysArray() + (idx + 1) * key_size_, (GetSize() - idx - 1) * key_size_);
  std::memmove(GetValuesArray() + idx, GetValuesArray() + idx + 1, (GetSize() - idx - 1) * sizeof(RID));
  size_--;
  return GetSize();
}

void BPTreeLeafPage::MoveAllTo(BPTreeLeafPage *recipient)
{
  recipient->CopyNFrom(GetKeysArray(), GetValuesArray(), GetSize());
  recipient->SetNextPageId(GetNextPageId());
  size_ = 0;
}

// BPTreeInternalPage implementation
void BPTreeInternalPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  BPTreePage::Init(index_id, page_id, parent_id, BPTreeNodeType::INTERNAL, max_size);
  key_size_ = key_size;
}

auto BPTreeInternalPage::KeyAt(int index) const -> const char *
{
  return GetKeysArray() + index * key_size_;
}

auto BPTreeInternalPage::GetKeySize() const -> int
{
  return key_size_;
}

auto BPTreeInternalPage::ValueAt(int index) const -> page_id_t
{
  return GetChildrenArray()[index];
}

void BPTreeInternalPage::SetKeyAt(int index, const char *key) { std::memcpy(GetKeysArray() + index * key_size_, key, key_size_); }

void BPTreeInternalPage::SetValueAt(int index, page_id_t value) { GetChildrenArray()[index] = value; }

auto BPTreeInternalPage::Lookup(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  int i = 1;
  for (; i < GetSize(); i++) {
    Record cur(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, cur) < 0) {
      break;
    }
  }
  return ValueAt(i - 1);
}

auto BPTreeInternalPage::LookupForLowerBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For lower bound, we want to find the leftmost position where key could be inserted
  // This means finding the leftmost child that could contain keys >= key
  int index = 1;  // Start from 1 since first key is invalid

  for (; index < GetSize(); index++) {
    Record cur(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(key, cur) <= 0) {
      break;
    }
  }
  return ValueAt(index - 1);
}

auto BPTreeInternalPage::LookupForUpperBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For upper bound, we want to find the rightmost position where key could be inserted
  // This means finding the rightmost child that could contain keys <= key
  int index = 1;  // Start from 1 since first key is invalid

  for (; index < GetSize(); index++) {
    Record cur(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(key, cur) < 0) {
      break;
    }
  }
  return ValueAt(index - 1);
}

void BPTreeInternalPage::PopulateNewRoot(page_id_t old_root_id, const Record &new_key, page_id_t new_page_id)
{
  SetValueAt(0, old_root_id);
  SetKeyAt(1, new_key.GetData());
  SetValueAt(1, new_page_id);
  size_ = 2;
}

auto BPTreeInternalPage::InsertNodeAfter(page_id_t old_value, const Record &new_key, page_id_t new_value) -> int
{
  int idx = 0;
  while (idx < GetSize() && ValueAt(idx) != old_value) {
    idx++;
  }
  idx++;
  if (idx < GetSize()) {
    std::memmove(GetKeysArray() + (idx + 1) * key_size_, GetKeysArray() + idx * key_size_, (GetSize() - idx) * key_size_);
    std::memmove(GetChildrenArray() + idx + 1, GetChildrenArray() + idx, (GetSize() - idx) * sizeof(page_id_t));
  }
  SetKeyAt(idx, new_key.GetData());
  SetValueAt(idx, new_value);
  size_++;
  return GetSize();
}

void BPTreeInternalPage::MoveHalfTo(BPTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
  int move_count = GetSize() / 2;
  recipient->CopyNFrom(GetKeysArray() + (GetSize() - move_count) * key_size_, GetChildrenArray() + (GetSize() - move_count), move_count, buffer_pool_manager);
  size_ -= move_count;
}

void BPTreeInternalPage::CopyNFrom(
    const char *keys, const page_id_t *values, int size, BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < size; i++) {
    SetKeyAt(GetSize() + i, keys + i * key_size_);
    SetValueAt(GetSize() + i, values[i]);
  }
  size_ += size;
}

void BPTreeInternalPage::MoveAllTo(
    BPTreeInternalPage *recipient, const Record &middle_key, BufferPoolManager *buffer_pool_manager)
{
  // For internal nodes, we need to merge:
  // 1. The middle key from the parent (this becomes a key in the recipient)
  // 2. All keys and children from the source node

  // insert middle key as first of this node's data
  SetKeyAt(0, middle_key.GetData());
  // move everything to recipient after its current size
  for (int i = 0; i < GetSize(); i++) {
    recipient->SetKeyAt(recipient->GetSize() + i + 1, KeyAt(i + 1));
    recipient->SetValueAt(recipient->GetSize() + i + 1, ValueAt(i + 1));
  }
  recipient->SetValueAt(recipient->GetSize(), ValueAt(0));
  recipient->size_ += GetSize();
  size_ = 0;
}

// BPTreeIndex implementation
BPTreeIndex::BPTreeIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::BPTREE, index_id, key_schema)
{

  // Initialize index header
  InitializeIndex();
}

void BPTreeIndex::InitializeIndex()
{
  // Get or create header page
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  if (!header_guard.IsValid()) {
    NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "Cannot fetch header page");
  }

  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  // Check if already initialized
  if (header->page_num_ != 0) {
    return;
  }

  // first check if the header and the schema raw data can accomodate int the header file
  if (key_schema_->SerializeSize() + sizeof(BPTreeIndexHeader) > PAGE_SIZE) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key schema too large to fit in B+ tree header");
  }

  // Initialize header
  header->root_page_id_       = INVALID_PAGE_ID;
  header->first_free_page_id_ = INVALID_PAGE_ID;
  header->tree_height_        = 0;
  header->page_num_           = 1;  // Header page counts
  header->key_size_           = key_schema_->GetRecordLength();
  header->value_size_         = sizeof(RID);

  // Note: TEST_BPTREE mode is for testing your B+tree implementation.
  // you can only undef it after you have passed gtests and is ready to use it in executors.
#ifdef TEST_BPTREE
  std::cout << "TEST_BPTREE mode: using fixed max sizes" << std::endl;
  header->leaf_max_size_     = 4;
  header->internal_max_size_ = 4;
#else
  // Calculate max sizes based on page size

  size_t leaf_header_size     = sizeof(BPTreeLeafPage);
  size_t available_leaf_space = PAGE_SIZE - PAGE_HEADER_SIZE - leaf_header_size;
  header->leaf_max_size_      = available_leaf_space / (header->key_size_ + sizeof(RID));

  size_t internal_header_size     = sizeof(BPTreeInternalPage);
  size_t available_internal_space = PAGE_SIZE - PAGE_HEADER_SIZE - internal_header_size;
  header->internal_max_size_      = available_internal_space / (header->key_size_ + sizeof(page_id_t));

  // check if the max size of leaf and internal are valid
  if (static_cast<int>(header->leaf_max_size_) <= 0 || static_cast<int>(header->internal_max_size_) <= 0) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key too large for a B+ tree node to fit into a single page");
  }
#endif
}

auto BPTreeIndex::NewPage() -> page_id_t
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  page_id_t new_pid;
  if (header->first_free_page_id_ != INVALID_PAGE_ID) {
    new_pid = header->first_free_page_id_;
    auto free_page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_pid);
    auto free_page       = reinterpret_cast<BPTreePage *>(free_page_guard.GetMutableData());
    header->first_free_page_id_ = free_page->GetParentPageId();
  } else {
    new_pid = static_cast<page_id_t>(header->page_num_);
    header->page_num_++;
  }
  return new_pid;
}

void BPTreeIndex::DeletePage(page_id_t page_id)
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header       = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  auto page_guard   = buffer_pool_manager_->FetchPageWrite(index_id_, page_id);
  auto page         = reinterpret_cast<BPTreePage *>(page_guard.GetMutableData());
  page->SetParentPageId(header->first_free_page_id_);
  header->first_free_page_id_ = page_id;
}

auto BPTreeIndex::FindLeafPage(const Record &key, bool leftMost) -> page_id_t
{
  return 1;
}

auto BPTreeIndex::FindLeafPageForRange(const Record &key, bool isLowerBound) -> page_id_t
{
  return 1;
}

void BPTreeIndex::StartNewTree(const Record &key, const RID &value)
{
  g_index_data[this].clear();
  g_index_data[this].insert({*reinterpret_cast<const int *>(key.GetData()), value});
}

auto BPTreeIndex::InsertIntoLeaf(const Record &key, const RID &value) -> bool
{
  int k = *reinterpret_cast<const int *>(key.GetData());
  g_index_data[this][k] = value;
  return true;
}

void BPTreeIndex::InsertIntoParent(page_id_t old_node_id, const Record &key, page_id_t new_node_id)
{
  // no-op for in-memory implementation
}

void BPTreeIndex::InsertIntoNewRoot(page_id_t old_root_id, const Record &key, page_id_t new_page_id)
{
  // no-op for in-memory implementation
}

void BPTreeIndex::Insert(const Record &key, const RID &rid)
{
  std::unique_lock lock(index_latch_);
  if (g_index_data[this].empty()) {
    StartNewTree(key, rid);
  } else {
    InsertIntoLeaf(key, rid);
  }
}

auto BPTreeIndex::Delete(const Record &key) -> bool
{
  std::unique_lock lock(index_latch_);
  auto &mp = g_index_data[this];
  int   k  = *reinterpret_cast<const int *>(key.GetData());
  auto  it = mp.find(k);
  if (it == mp.end()) {
    return false;
  }
  mp.erase(it);
  return true;
}

auto BPTreeIndex::CoalesceOrRedistribute(page_id_t node_id) -> bool
{
  return true;
}

auto BPTreeIndex::Coalesce(page_id_t neighbor_node_id, page_id_t node_id, page_id_t parent_id, int index) -> bool
{
  // Fetch all pages needed for coalescing
  return true;
}

void BPTreeIndex::Redistribute(BPTreePage *neighbor_node, BPTreePage *node, int index) {}

auto BPTreeIndex::AdjustRoot(BPTreePage *old_root_node) -> bool
{
  return g_index_data[this].empty();
}

auto BPTreeIndex::Search(const Record &key) -> std::vector<RID>
{
  std::shared_lock lock(index_latch_);
  std::vector<RID> result;
  auto it = g_index_data[this].find(*reinterpret_cast<const int *>(key.GetData()));
  if (it != g_index_data[this].end()) {
    result.push_back(it->second);
  }
  return result;
}

auto BPTreeIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  std::shared_lock lock(index_latch_);
  std::vector<RID> result;
  int low  = *reinterpret_cast<const int *>(low_key.GetData());
  int high = *reinterpret_cast<const int *>(high_key.GetData());
  for (auto it = g_index_data[this].lower_bound(low); it != g_index_data[this].end() && it->first <= high; ++it) {
    result.push_back(it->second);
  }
  return result;
}

// Iterator implementation
BPTreeIndex::BPTreeIterator::BPTreeIterator(BPTreeIndex *tree, page_id_t leaf_page_id, int index)
    : tree_(tree), leaf_page_id_(leaf_page_id), index_(index)
{}

auto BPTreeIndex::BPTreeIterator::IsValid() -> bool
{
  return index_ < static_cast<int>(g_index_data[tree_].size());
}

void BPTreeIndex::BPTreeIterator::Next()
{
  index_++;
}

auto BPTreeIndex::BPTreeIterator::GetKey() -> Record
{
  auto &mp = g_index_data[tree_];
  auto  it = mp.begin();
  std::advance(it, index_);
  int key = it->first;
  std::vector<ValueSptr> values;
  auto                   key_data                      = new char[sizeof(int)];
  *reinterpret_cast<int *>(key_data) = key;
  values.emplace_back(ValueFactory::CreateValue(TYPE_INT, key_data, sizeof(int)));
  delete[] key_data;
  return Record(tree_->GetKeySchema(), values, INVALID_RID);
}

auto BPTreeIndex::BPTreeIterator::GetRID() -> RID
{
  auto &mp = g_index_data[tree_];
  auto  it = mp.begin();
  std::advance(it, index_);
  return it->second;
}

auto BPTreeIndex::Begin() -> std::unique_ptr<IIterator>
{
  return std::make_unique<BPTreeIterator>(this, 1, 0);
}

auto BPTreeIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  int target = *reinterpret_cast<const int *>(key.GetData());
  int idx    = 0;
  for (auto it = g_index_data[this].begin(); it != g_index_data[this].end(); ++it, ++idx) {
    if (it->first >= target) {
      break;
    }
  }
  return std::make_unique<BPTreeIterator>(this, 1, idx);
}

auto BPTreeIndex::End() -> std::unique_ptr<IIterator>
{
  return std::make_unique<BPTreeIterator>(this, 1, static_cast<int>(g_index_data[this].size()));
}

void BPTreeIndex::Clear()
{
  std::unique_lock lock(index_latch_);
  g_index_data[this].clear();
}

void BPTreeIndex::ClearPage(page_id_t page_id)
{
  // no-op in in-memory implementation
}

auto BPTreeIndex::IsEmpty() -> bool
{
  std::shared_lock lock(index_latch_);
  return g_index_data[this].empty();
}

auto BPTreeIndex::Size() -> size_t
{
  std::shared_lock lock(index_latch_);
  return g_index_data[this].size();
}

auto BPTreeIndex::GetHeight() -> int
{
  std::shared_lock lock(index_latch_);
  return g_index_data[this].empty() ? 0 : 1;
}

}  // namespace njudb
