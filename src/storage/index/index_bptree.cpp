
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

#include "index_bptree.h"
#include "../../../common/error.h"
#include "../buffer/page_guard.h"
#include <algorithm>
#include <cstring>
#include <vector>
#include <string>

// #define TEST_BPTREE

namespace njudb {

// BPTreePage implementation
void BPTreePage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, BPTreeNodeType node_type, int max_size)
{
  index_id_ = index_id;
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  node_type_ = node_type;
  size_ = 0;
  max_size_ = max_size;
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
  return size_;
}

auto BPTreePage::GetMaxSize() const -> int
{
  return max_size_;
}

void BPTreePage::SetSize(int size)
{
  size_ = size;
}

auto BPTreePage::GetPageId() const -> page_id_t
{
  return page_id_;
}

auto BPTreePage::GetParentPageId() const -> page_id_t
{
  return parent_page_id_;
}

void BPTreePage::SetParentPageId(page_id_t parent_page_id)
{
  parent_page_id_ = parent_page_id;
}

auto BPTreePage::IsSafe(bool is_insert) const -> bool
{
  if (is_insert) {
    return size_ < max_size_;
  }
  if (IsLeaf()) {
    return size_ > max_size_ / 2;
  }
  return size_ > (max_size_ + 1) / 2;
}

// BPTreeLeafPage implementation
void BPTreeLeafPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  BPTreePage::Init(index_id, page_id, parent_id, BPTreeNodeType::LEAF, max_size);
  next_page_id_ = INVALID_PAGE_ID;
  key_size_ = key_size;
}

auto BPTreeLeafPage::GetNextPageId() const -> page_id_t
{
  return next_page_id_;
}

void BPTreeLeafPage::SetNextPageId(page_id_t next_page_id)
{
  next_page_id_ = next_page_id;
}

auto BPTreeLeafPage::KeyAt(int index) const -> const char *
{
  if (index < 0 || index >= size_) {
    return nullptr;
  }
  return GetKeysArray() + index * key_size_;
}

auto BPTreeLeafPage::ValueAt(int index) const -> RID
{
  if (index < 0 || index >= size_) {
    return INVALID_RID;
  }
  return GetValuesArray()[index];
}

void BPTreeLeafPage::SetKeyAt(int index, const char *key)
{
  if (index < 0 || index >= max_size_) {
    return;
  }
  memcpy(GetKeysArray() + index * key_size_, key, key_size_);
}

void BPTreeLeafPage::SetValueAt(int index, const RID &value)
{
  if (index < 0 || index >= max_size_) {
    return;
  }
  GetValuesArray()[index] = value;
}

auto BPTreeLeafPage::KeyIndex(const Record &key, const RecordSchema *schema) const -> int
{
  // 线性查找第一个大于等于key的位置
  for (int i = 0; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    int cmp = Record::Compare(current_key, key);
    if (cmp >= 0) {
      return i;
    }
  }
  return size_;  // 如果所有键都小于key，则返回末尾
}

auto BPTreeLeafPage::LowerBound(const Record &key, const RecordSchema *schema) const -> int
{
  return KeyIndex(key, schema);
}

auto BPTreeLeafPage::UpperBound(const Record &key, const RecordSchema *schema) const -> int
{
  // 线性查找第一个大于key的位置
  for (int i = 0; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    int cmp = Record::Compare(current_key, key);
    if (cmp > 0) {
      return i;
    }
  }
  return size_;
}

auto BPTreeLeafPage::Lookup(const Record &key, const RecordSchema *schema) const -> std::vector<RID>
{
  std::vector<RID> result;
  int index = KeyIndex(key, schema);
  
  // Collect all matching keys
  while (index < size_) {
    Record current_key(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(current_key, key) == 0) {
      result.push_back(ValueAt(index));
      index++;
    } else {
      break;
    }
  }
  
  return result;
}

auto BPTreeLeafPage::Insert(const Record &key, const RID &value, const RecordSchema *schema) -> int
{
  // Check if leaf is full
  if (size_ >= max_size_) {
    return -1;
  }
  
  int insert_pos = KeyIndex(key, schema);
  
  // Shift elements to make space
  for (int i = size_; i > insert_pos; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  
  // Insert new element
  SetKeyAt(insert_pos, key.GetData());
  SetValueAt(insert_pos, value);
  size_++;
  
  return size_;
}

void BPTreeLeafPage::MoveHalfTo(BPTreeLeafPage *recipient)
{
  // Split point (ceil division)
  int split_index = (size_ + 1) / 2;
  int num_to_move = size_ - split_index;
  
  // Copy data to recipient
  for (int i = 0; i < num_to_move; i++) {
    recipient->SetKeyAt(i, KeyAt(split_index + i));
    recipient->SetValueAt(i, ValueAt(split_index + i));
  }
  
  recipient->SetSize(num_to_move);
  size_ = split_index;
}

void BPTreeLeafPage::CopyNFrom(const char *keys, const RID *values, int size)
{
  for (int i = 0; i < size; i++) {
    SetKeyAt(size_ + i, keys + i * key_size_);
    SetValueAt(size_ + i, values[i]);
  }
  size_ += size;
}

auto BPTreeLeafPage::RemoveRecord(const Record &key, const RecordSchema *schema) -> int
{
  int index = KeyIndex(key, schema);
  
  if (index < size_) {
    Record current_key(schema, nullptr, KeyAt(index), INVALID_RID);
    if (Record::Compare(current_key, key) == 0) {
      // Shift elements left
      for (int i = index; i < size_ - 1; i++) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
      }
      size_--;
      return index;
    }
  }
  
  return -1;
}

void BPTreeLeafPage::MoveAllTo(BPTreeLeafPage *recipient)
{
  recipient->CopyNFrom(GetKeysArray(), GetValuesArray(), size_);
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
  return GetKeysArray() + index* key_size_;
}

auto BPTreeInternalPage::GetKeySize() const -> int
{
  return key_size_;
}

void BPTreeInternalPage::SetKeyAt(int index, const char *key)
{
  memcpy(GetKeysArray() + index * key_size_, key, key_size_);
}

auto BPTreeInternalPage::ValueAt(int index) const -> page_id_t
{
  return GetChildrenArray()[index];
}

void BPTreeInternalPage::SetValueAt(int index, page_id_t value)
{
  GetChildrenArray()[index] = value;
}

auto BPTreeInternalPage::Lookup(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // 线性查找：找到第一个大于key的键，返回前一个子指针
  for (int i = 1; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) < 0) {
      return ValueAt(i - 1);
    }
  }
  return ValueAt(size_ - 1);  // 如果所有键都小于等于key，返回最后一个子指针
}

auto BPTreeInternalPage::LookupForLowerBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // 线性查找：找到第一个大于等于key的键，返回前一个子指针
  for (int i = 1; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) < 0) {
      return ValueAt(i - 1);
    }
  }
  return ValueAt(size_ - 1);
}

auto BPTreeInternalPage::LookupForUpperBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // 线性查找：找到第一个大于key的键，返回前一个子指针
  for (int i = 1; i < size_; i++) {
    Record current_key(schema, nullptr, KeyAt(i), INVALID_RID);
    if (Record::Compare(key, current_key) < 0) {
      return ValueAt(i - 1);
    }
  }
  return ValueAt(size_ - 1);
}

void BPTreeInternalPage::PopulateNewRoot(page_id_t old_root_id, const Record &new_key, page_id_t new_page_id)
{
  SetValueAt(0, old_root_id);
  SetKeyAt(1, new_key.GetData());
  SetValueAt(1, new_page_id);
  SetSize(2);
}

auto BPTreeInternalPage::InsertNodeAfter(page_id_t old_value, const Record &new_key, page_id_t new_value) -> int
{
  // printf("=== InsertNodeAfter Debug ===\n");
  // printf("Looking for old_value=%d in size=%d, max_size=%d\n", old_value, size_, max_size_);
  
  // Find position of old_value
  int old_index = -1;
  for (int i = 0; i < size_; i++) {
    if (ValueAt(i) == old_value) {
      old_index = i;
      //printf("Found old_value at index: %d\n", old_index);
      break;
    }
  }
  
  if (old_index == -1) {
    printf("ERROR: old_value %d not found!\n", old_value);
    return -1;
  }
  
  // New key goes at position old_index + 1
  int key_index = old_index + 1;
  //printf("New key will be inserted at key_index: %d\n", key_index);
  
  
  // Shift keys right (keys start at index 1)
  for (int i = size_ - 1; i >= key_index; i--) {
    const char* src_key = KeyAt(i);
    //printf("Shifting key[%d] to key[%d]\n", i, i + 1);
    SetKeyAt(i + 1, src_key);
  }
  
  // Shift values right
  for (int i = size_; i > old_index + 1; i--) {
    page_id_t src_value = ValueAt(i - 1);
    //printf("Shifting value[%d] to value[%d]\n", i - 1, i);
    SetValueAt(i, src_value);
  }
  
  
  SetKeyAt(key_index, new_key.GetData());
  SetValueAt(old_index + 1, new_value);
  
  // Update size
  SetSize(size_ + 1);
  
  return size_;
}

void BPTreeInternalPage::MoveHalfTo(BPTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
  // printf("=== MoveHalfTo Debug ===\n");
  // printf("Original node %d, size: %d\n", GetPageId(), size_);
  
  // 对于内部节点：有 size_ 个子指针，size_-1 个键
  int total_keys = size_ - 1;
  
  // 分裂点：ceil((size_)/2)
  int split_index = size_ / 2;
  if (size_ % 2 == 1) {
    split_index++;  // 向上取整
  }
  
  //printf("Split index (by children): %d, total keys: %d\n", split_index, total_keys);
  
  // 清空接收者
  recipient->SetSize(0);
  
  // 计算要移动的键数量
  // 原节点保留前 split_index 个子指针，剩下的给接收者
  int keys_to_move = total_keys - split_index;
  int children_to_move = size_ - split_index;
  
  //printf("Moving %d keys and %d children to recipient\n", keys_to_move, children_to_move);
  
  // 复制键到接收者（键从索引1开始）
  // 注意：键的分裂点与子指针不同
  // 原节点的第 split_index 个键会上移到父节点，所以不移动
  int dest_key_index = 1;
  for (int i = split_index + 1; i <= total_keys; i++) {
    const char* src_key = KeyAt(i);
    if (src_key != nullptr) {
      //printf("Moving key[%d] to recipient key[%d]\n", i, dest_key_index);
      recipient->SetKeyAt(dest_key_index, src_key);
      dest_key_index++;
    } else {
      //printf("WARNING: KeyAt(%d) is null!\n", i);
    }
  }
  
  // 复制子指针到接收者
  int dest_child_index = 0;
  for (int i = split_index; i < size_; i++) {
    page_id_t child_id = ValueAt(i);
    //printf("Moving child[%d] (page_id=%d) to recipient child[%d]\n", i, child_id, dest_child_index);
    recipient->SetValueAt(dest_child_index, child_id);
    dest_child_index++;
  }
  
  // 设置接收者大小
  recipient->SetSize(children_to_move);
  //printf("Recipient %d new size: %d\n", recipient->GetPageId(), recipient->GetSize());
  
  // 更新原节点大小
  SetSize(split_index);
  //printf("Original node %d new size: %d\n", GetPageId(), size_);
  
  
  // 更新移动的子节点的父指针
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id_t child_page_id = recipient->ValueAt(i);
    if (child_page_id != INVALID_PAGE_ID) {
      auto child_guard = buffer_pool_manager->FetchPageWrite(index_id_, child_page_id);
      if (child_guard.IsValid()) {
        auto child_page = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
        child_page->SetParentPageId(recipient->GetPageId());
        // printf("Updated parent of child page %d from %d to %d\n", 
        //        child_page_id, GetPageId(), recipient->GetPageId());
      }
    }
  }
  
  //printf("=== MoveHalfTo completed ===\n");
}
void BPTreeInternalPage::CopyNFrom(const char *keys, const page_id_t *values, int size, BufferPoolManager *buffer_pool_manager)
{
  // Copy keys (starting from index 1)
  for (int i = 0; i < size; i++) {
    SetKeyAt(size_ + i + 1, keys + i * key_size_);
  }
  
  // Copy child pointers
  for (int i = 0; i <= size; i++) {
    SetValueAt(size_ + i, values[i]);
  }
  
  SetSize(size_ + size + 1);
  
  // Update parent pointers
  for (int i = size_; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    if (child_page_id != INVALID_PAGE_ID) {
      auto child_guard = buffer_pool_manager->FetchPageWrite(index_id_, child_page_id);
      auto child_page = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
      child_page->SetParentPageId(GetPageId());
    }
  }
}

void BPTreeInternalPage::MoveAllTo(BPTreeInternalPage *recipient, const Record &middle_key, BufferPoolManager *buffer_pool_manager)
{
  // Insert middle key first
  recipient->SetKeyAt(recipient->GetSize(), middle_key.GetData());
  
  // Copy all keys (starting from index 1)
  for (int i = 1; i < size_; i++) {
    recipient->SetKeyAt(recipient->GetSize() + i, KeyAt(i));
  }
  
  // Copy all child pointers
  for (int i = 0; i < size_; i++) {
    recipient->SetValueAt(recipient->GetSize() + i, ValueAt(i));
  }
  
  recipient->SetSize(recipient->GetSize() + size_);
  SetSize(0);
  
  // Update parent pointers
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id_t child_page_id = recipient->ValueAt(i);
    if (child_page_id != INVALID_PAGE_ID) {
      auto child_guard = buffer_pool_manager->FetchPageWrite(index_id_, child_page_id);
      auto child_page = reinterpret_cast<BPTreePage *>(PageContentPtr(child_guard.GetMutableData()));
      child_page->SetParentPageId(recipient->GetPageId());
    }
  }
}

// BPTreeIndex implementation
BPTreeIndex::BPTreeIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::BPTREE, index_id, key_schema)
{
  InitializeIndex();
}

void BPTreeIndex::InitializeIndex()
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  if (!header_guard.IsValid()) {
    NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "Cannot fetch header page");
  }

  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  if (header->page_num_ != 0) {
    return;
  }

  if (key_schema_->SerializeSize() + sizeof(BPTreeIndexHeader) > PAGE_SIZE) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key schema too large to fit in B+ tree header");
  }

  // Initialize header
  header->root_page_id_ = INVALID_PAGE_ID;
  header->first_free_page_id_ = INVALID_PAGE_ID;
  header->tree_height_ = 0;
  header->page_num_ = 1;
  header->key_size_ = key_schema_->GetRecordLength();
  header->value_size_ = sizeof(RID);

#ifdef TEST_BPTREE
  header->leaf_max_size_ = 4;
  header->internal_max_size_ = 4;
#else
  size_t leaf_header_size = sizeof(BPTreeLeafPage);
  size_t available_leaf_space = PAGE_SIZE - PAGE_HEADER_SIZE - leaf_header_size;
  header->leaf_max_size_ = available_leaf_space / (header->key_size_ + sizeof(RID));

  size_t internal_header_size = sizeof(BPTreeInternalPage);
  size_t available_internal_space = PAGE_SIZE - PAGE_HEADER_SIZE - internal_header_size;
  header->internal_max_size_ = available_internal_space / (header->key_size_ + sizeof(page_id_t));

  if (static_cast<int>(header->leaf_max_size_) <= 0 || static_cast<int>(header->internal_max_size_) <= 0) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key too large for a B+ tree node to fit into a single page");
  }
#endif
}

auto BPTreeIndex::NewPage() -> page_id_t
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  page_id_t new_page_id;
  
  // 检查是否有空闲页面可重用
  if (header->first_free_page_id_ != INVALID_PAGE_ID) {
    new_page_id = header->first_free_page_id_;

    // 获取空闲页面来读取下一个空闲页面ID
    auto free_page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_page_id);

    // 正确方式2：如果页面内容布局已知，第一个 sizeof(page_id_t) 存储 next_free
    page_id_t next_free = *reinterpret_cast<page_id_t *>(free_page_guard.GetMutableData() + PAGE_NEXT_FREE_PAGE_ID_OFFSET);
    header->first_free_page_id_ = next_free;
    
  } else {
    // 分配新页面
    new_page_id = header->page_num_;
    header->page_num_++;
  }
  
  return new_page_id;
}

void BPTreeIndex::DeletePage(page_id_t page_id)
{
  //std::unique_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, page_id);
  auto page =(page_guard.GetMutableData());
  
  *reinterpret_cast<page_id_t *>(page + PAGE_NEXT_FREE_PAGE_ID_OFFSET) = header->first_free_page_id_;
  header->first_free_page_id_ = page_id;
  
  //memset(PageContentPtr(page_guard.GetMutableData()), 0, PAGE_SIZE - PAGE_HEADER_SIZE);
}


auto BPTreeIndex::FindLeafPage(const Record &key, bool leftMost) -> page_id_t
{
  //std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  
  page_id_t current_page_id = header->root_page_id_;
  
  for (size_t level = 0; level < header->tree_height_; level++) {
    auto current_guard = buffer_pool_manager_->FetchPageRead(index_id_, current_page_id);
    auto data_ptr = current_guard.GetData();
    auto current_page = reinterpret_cast<const BPTreePage *>(PageContentPtr(data_ptr));
    
    if (current_page->IsLeaf()) {
      return current_page_id;
    }
    
    auto internal_page = reinterpret_cast<const BPTreeInternalPage *>(PageContentPtr(data_ptr));
    
    if (leftMost) {
      current_page_id = internal_page->ValueAt(0);
    } else {
      current_page_id = internal_page->Lookup(key, key_schema_);
    }
  }
  
  return INVALID_PAGE_ID;
}

auto BPTreeIndex::FindLeafPageForRange(const Record &key, bool isLowerBound) -> page_id_t
{
  //std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  
  page_id_t current_page_id = header->root_page_id_;
  
  for (size_t level = 0; level < header->tree_height_; level++) {
    auto current_guard = buffer_pool_manager_->FetchPageRead(index_id_, current_page_id);
    auto data_ptr = current_guard.GetData();
    auto current_page = reinterpret_cast<const BPTreePage *>(PageContentPtr(data_ptr));
    
    if (current_page->IsLeaf()) {
      return current_page_id;
    }
    
    auto internal_page = reinterpret_cast<const BPTreeInternalPage *>(PageContentPtr(data_ptr));
    
    if (isLowerBound) {
      current_page_id = internal_page->LookupForLowerBound(key, key_schema_);
    } else {
      current_page_id = internal_page->LookupForUpperBound(key, key_schema_);
    }
  }
  
  return INVALID_PAGE_ID;
}

void BPTreeIndex::StartNewTree(const Record &key, const RID &value)
{
  //std::unique_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  page_id_t root_page_id = NewPage();
  // printf("Creating new tree with root leaf page %d\n", root_page_id);
  
  auto root_guard = buffer_pool_manager_->FetchPageWrite(index_id_, root_page_id);
  auto root_page = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(root_guard.GetMutableData()));
  
  root_page->Init(index_id_, root_page_id, INVALID_PAGE_ID, header->key_size_, header->leaf_max_size_);

  root_page->Insert(key, value, key_schema_);


  // header->root_page_id_ = root_page_id;
  // header->tree_height_ = 1;
  // header->num_entries_ = 1;
  
  auto writeguardforheader = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  (reinterpret_cast<BPTreeIndexHeader *>(writeguardforheader.GetMutableData()))->root_page_id_ = root_page_id;
  (reinterpret_cast<BPTreeIndexHeader *>(writeguardforheader.GetMutableData()))->tree_height_ = 1;
  //(reinterpret_cast<BPTreeIndexHeader *>(writeguardforheader.GetMutableData()))->num_entries_ = 1;
}

auto BPTreeIndex::InsertIntoLeaf(const Record &key, const RID &value) -> bool
{
  page_id_t leaf_page_id = FindLeafPage(key,false);
  
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  auto leaf_guard = buffer_pool_manager_->FetchPageWrite(index_id_, leaf_page_id);
  auto leaf_page = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetMutableData()));
  
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  // // 调试：插入前打印叶子节点状态
  // DebugPrintLeaf(leaf_page, key_schema_);
  
  // Check if leaf has space
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    // Direct insertion
    int insert_pos = leaf_page->Insert(key, value, key_schema_);
    if (insert_pos == -1) {
      return false;
    }
    
    header->num_entries_++;
    
    // // 调试：插入后打印叶子节点状态
    // DebugPrintLeaf(leaf_page, key_schema_);
    
    return true;
  } else {

    
    // Create new leaf page
    page_id_t new_leaf_page_id = NewPage();
    
    auto new_leaf_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_leaf_page_id);
    auto new_leaf_page = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(new_leaf_guard.GetMutableData()));
    
    // Initialize new leaf
    new_leaf_page->Init(index_id_, new_leaf_page_id, leaf_page->GetParentPageId(),
                       header->key_size_, header->leaf_max_size_);
    
    // Update sibling pointers
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_leaf_page_id);
    
    // Create temporary arrays for all keys (original + new)
    int total_keys = leaf_page->GetSize() + 1;
    std::vector<char> all_keys(total_keys * header->key_size_);
    std::vector<RID> all_values(total_keys);
    
    // Copy original keys
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      memcpy(all_keys.data() + i * header->key_size_, 
             leaf_page->KeyAt(i), header->key_size_);
      all_values[i] = leaf_page->ValueAt(i);
    }
    
    // Find insertion position for new key
    int insert_pos = 0;
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      Record current_key(key_schema_, nullptr, leaf_page->KeyAt(i), INVALID_RID);
      if (Record::Compare(current_key, key) < 0) {
        insert_pos++;
      } else {
        break;
      }
    }
    
    
    // Make space for new key
    for (int i = total_keys - 1; i > insert_pos; i--) {
      memcpy(all_keys.data() + i * header->key_size_,
             all_keys.data() + (i - 1) * header->key_size_, header->key_size_);
      all_values[i] = all_values[i - 1];
    }
    
    // Insert new key
    memcpy(all_keys.data() + insert_pos * header->key_size_, key.GetData(), header->key_size_);
    all_values[insert_pos] = value;
    
    // // 调试：打印排序后的所有键
    // printf("All keys in sorted order: ");
    // for (int i = 0; i < total_keys; i++) {
    //   Record current_key(key_schema_, nullptr, all_keys.data() + i * header->key_size_, INVALID_RID);
    //   printf("%s ", current_key.GetValueAt(0)->ToString().c_str());
    // }
    // printf("\n");
    
    // Split point (ceil division)
    int split_index = (total_keys + 1) / 2;
    
    
    // Clear original leaf
    leaf_page->SetSize(0);
    
    // Copy first half to original leaf
    for (int i = 0; i < split_index; i++) {
      leaf_page->SetKeyAt(i, all_keys.data() + i * header->key_size_);
      leaf_page->SetValueAt(i, all_values[i]);
    }
    leaf_page->SetSize(split_index);
    
    // Copy second half to new leaf
    for (int i = split_index; i < total_keys; i++) {
      new_leaf_page->SetKeyAt(i - split_index, all_keys.data() + i * header->key_size_);
      new_leaf_page->SetValueAt(i - split_index, all_values[i]);
    }
    new_leaf_page->SetSize(total_keys - split_index);
    
    // Get first key of new leaf as separator
    Record separator_key(key_schema_, nullptr, new_leaf_page->KeyAt(0), INVALID_RID);
    
    header->num_entries_++;
  
    
    // Insert separator key into parent
    InsertIntoParent(leaf_page_id, separator_key, new_leaf_page_id);
    
    return true;
  }
}

void BPTreeIndex::InsertIntoParent(page_id_t old_node_id, const Record &key, page_id_t new_node_id)
{
  auto old_guard = buffer_pool_manager_->FetchPageWrite(index_id_, old_node_id);
  auto old_node = reinterpret_cast<BPTreePage *>(PageContentPtr(old_guard.GetData()));
  

  // Check if old node is root
  if (old_node->IsRoot()) {
    InsertIntoNewRoot(old_node_id, key, new_node_id);
    return;
  }
  
  // Get parent node
  page_id_t parent_page_id = old_node->GetParentPageId();
  old_guard.Drop();
  
  auto parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, parent_page_id);
  auto parent_page = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());


  // Check if parent has space
  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    // Parent has space, insert directly
    //printf("Parent has space, inserting directly\n");
    int result = parent_page->InsertNodeAfter(old_node_id, key, new_node_id);
    //printf("InsertNodeAfter returned: %d\n", result);
    
    // Update parent pointer of new node
    auto new_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_node_id);
    auto new_node_page = reinterpret_cast<BPTreePage *>(PageContentPtr(new_guard.GetMutableData()));
    new_node_page->SetParentPageId(parent_page_id);
    return;
  } else {
    parent_page->InsertNodeAfter(old_node_id, key, new_node_id);
  
    // Update new node's parent pointer before split
    auto new_node_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_node_id);
    auto new_node = reinterpret_cast<BPTreePage *>(PageContentPtr(new_node_guard.GetMutableData()));
    new_node->SetParentPageId(parent_page_id);
    new_node_guard.Drop();
    
    page_id_t new_parent_page_id = NewPage();
    
    auto new_parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_parent_page_id);
    auto new_parent_page = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(new_parent_guard.GetMutableData()));

    new_parent_page->Init(index_id_, new_parent_page_id, parent_page->GetParentPageId(),
                         header->key_size_, header->internal_max_size_);

    
    // // Split the parent
    // //printf("\n--- Starting parent split ---\n");
    // parent_page->MoveHalfTo(new_parent_page, buffer_pool_manager_);
    // //printf("--- Parent split completed ---\n\n");
  int split_index = parent_page->GetSize() / 2;
  int num_to_move = parent_page->GetSize() - split_index;
  int size = parent_page->GetSize();
  Record middle_key(key_schema_, nullptr, parent_page->KeyAt(split_index), INVALID_RID);

  new_parent_page->SetValueAt(0, parent_page->ValueAt(split_index));
  for (int i = split_index + 1; i < size; i++) {
    new_parent_page->SetKeyAt(i - split_index, parent_page->KeyAt(i));
    new_parent_page->SetValueAt(i - split_index, parent_page->ValueAt(i));
  }

  // Update parent pointers of moved children
  for (int i = 0; i < num_to_move; i++) {
    page_id_t child_page_id = new_parent_page->ValueAt(i);
    auto children_guard = buffer_pool_manager_->FetchPageWrite(index_id_, child_page_id);
    auto children_node = reinterpret_cast<BPTreePage *>(PageContentPtr(children_guard.GetMutableData()));
    children_node->SetParentPageId(new_parent_page_id);
  }
  
  // Update parent size (excluding the middle key that will be pushed up)
  new_parent_page->SetSize(num_to_move);
  parent_page->SetSize(split_index);
  
  parent_guard.Drop();
  new_parent_guard.Drop();
  
  InsertIntoParent(parent_page->GetPageId(), middle_key, new_parent_page_id);
}
  //   // 获取中间键
  //   // 中间键应该是新父节点的第一个键
  //   if (new_parent_page->GetSize() > 1) {
  //     const char* middle_key_ptr = new_parent_page->KeyAt(1);
  //     if (middle_key_ptr != nullptr) {
  //       Record middle_key(key_schema_, nullptr, middle_key_ptr, INVALID_RID);
        
  //       // 递归处理父节点的父节点
  //       InsertIntoParent(parent_page_id, middle_key, new_parent_page_id);
  //     } else {
  //       if (parent_page->GetSize() > 1) {
  //         const char* alt_key_ptr = parent_page->KeyAt(parent_page->GetSize() - 1);
  //         if (alt_key_ptr != nullptr) {
  //           Record alt_key(key_schema_, nullptr, alt_key_ptr, INVALID_RID);

  //           InsertIntoParent(parent_page_id, alt_key, new_parent_page_id);
  //         } else {
  //           InsertIntoParent(parent_page_id, key, new_parent_page_id);
  //         }
  //       } else {
  //         InsertIntoParent(parent_page_id, key, new_parent_page_id);
  //       }
  //     }
  //   } else {
  //     // printf("ERROR: New parent has insufficient size (%d)! Using original key.\n", 
  //     //        new_parent_page->GetSize());
  //     InsertIntoParent(parent_page_id, key, new_parent_page_id);
  //   }
  // }
}


void BPTreeIndex::InsertIntoNewRoot(page_id_t old_root_id, const Record &key, page_id_t new_page_id)
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  // Create new root page
  page_id_t new_root_page_id = NewPage();
  //printf("Creating new root page %d\n", new_root_page_id);
  
  auto new_root_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_root_page_id);
  
  // 确保页面获取成功
  if (!new_root_guard.IsValid()) {
    //printf("ERROR: Failed to fetch new root page %d\n", new_root_page_id);
    return;
  }
  
  auto new_root_page = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(new_root_guard.GetMutableData()));
  
  // 检查指针是否有效
  if (new_root_page == nullptr) {
    ///printf("ERROR: Failed to cast new root page\n");
    return;
  }
  
  new_root_page->Init(index_id_, new_root_page_id, INVALID_PAGE_ID,
                     header->key_size_, header->internal_max_size_);
  
  // Set up new root
  new_root_page->PopulateNewRoot(old_root_id, key, new_page_id);
  
  // Update parent pointers
  auto old_root_guard = buffer_pool_manager_->FetchPageWrite(index_id_, old_root_id);
  if (!old_root_guard.IsValid()) {
    //printf("ERROR: Failed to fetch old root page %d\n", old_root_id);
    return;
  }
  
  auto old_root_page = reinterpret_cast<BPTreePage *>(PageContentPtr(old_root_guard.GetMutableData()));
  if (old_root_page != nullptr) {
    old_root_page->SetParentPageId(new_root_page_id);
  }
  
  auto new_page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_page_id);
  if (!new_page_guard.IsValid()) {
    //printf("ERROR: Failed to fetch new page %d\n", new_page_id);
    return;
  }
  
  auto new_page_node = reinterpret_cast<BPTreePage *>(PageContentPtr(new_page_guard.GetMutableData()));
  if (new_page_node != nullptr) {
    new_page_node->SetParentPageId(new_root_page_id);
  }
  
  // Update header
  header->root_page_id_ = new_root_page_id;
  header->tree_height_++;

}


void BPTreeIndex::Insert(const Record &key, const RID &rid)
{
  // printf("\n=== Starting Insert: key=%s, RID=(%d,%d) ===\n", 
  //        key.GetValueAt(0)->ToString().c_str(), rid.PageID(), rid.SlotID());
  
  //std::unique_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  
  
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    //printf("Tree is empty, creating new tree\n");
    StartNewTree(key, rid);
  } else {
    InsertIntoLeaf(key, rid);
  }
  
  // printf("=== Insert completed ===\n\n");
}

auto BPTreeIndex::Delete(const Record &key) -> bool
{
  std::unique_lock lock(index_latch_);
  
  // Find leaf page containing the key
  page_id_t leaf_page_id = FindLeafPage(key,false);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  auto leaf_guard = buffer_pool_manager_->FetchPageWrite(index_id_, leaf_page_id);
  auto leaf_page = reinterpret_cast<BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetMutableData()));
  
  // Remove the record
  int remove_pos = leaf_page->RemoveRecord(key, key_schema_);
  if (remove_pos == -1) {
    return false;
  }
  
  // Update entry count
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  //header->num_entries_--;
  
  // // Check if leaf is underflowing
  // if (!leaf_page->IsSafe(false)) {
  //   CoalesceOrRedistribute(leaf_page_id);
  // }
  if(leaf_page->IsRoot())
  {
    if (leaf_page->GetSize() > 1) {
      leaf_page->RemoveRecord(key, key_schema_);
      return true;
    }
    auto writeguardforheader = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
    (reinterpret_cast<BPTreeIndexHeader *>(writeguardforheader.GetMutableData()))->root_page_id_ = INVALID_PAGE_ID;
    (reinterpret_cast<BPTreeIndexHeader *>(writeguardforheader.GetMutableData()))->tree_height_ = 0;
    DeletePage(leaf_page_id);
    return true;
  }
  if(!leaf_page->IsSafe(false) && !leaf_page->IsRoot())
  {
    //printf("Leaf page %d is underflowing, needs coalesce or redistribute\n", leaf_page_id);
    CoalesceOrRedistribute(leaf_page_id);
    return true;
  }
  return true;
}

auto BPTreeIndex::CoalesceOrRedistribute(page_id_t node_id) -> bool
{
  // 先读取节点信息
  auto node_read_guard = buffer_pool_manager_->FetchPageRead(index_id_, node_id);
  auto node_page = reinterpret_cast<const BPTreePage *>(PageContentPtr(node_read_guard.GetData()));
  
  // 处理根节点情况
  if (node_page->IsRoot()) {
    node_read_guard.Drop();
    auto node_write_guard = buffer_pool_manager_->FetchPageWrite(index_id_, node_id);
    auto node_write_page = reinterpret_cast<BPTreePage *>(PageContentPtr(node_write_guard.GetMutableData()));
    return AdjustRoot(node_write_page);
  }
  
  // 获取父节点
  page_id_t parent_page_id = node_page->GetParentPageId();
  node_read_guard.Drop();
  
  auto parent_read_guard = buffer_pool_manager_->FetchPageRead(index_id_, parent_page_id);
  auto parent_page = reinterpret_cast<const BPTreeInternalPage *>(PageContentPtr(parent_read_guard.GetData()));
  
  // 在父节点中查找当前节点的位置
  int current_pos = 0;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (parent_page->ValueAt(i) == node_id) {
      current_pos = i;
      break;
    }
  }
  
  // 选择兄弟节点
  page_id_t sibling_id = INVALID_PAGE_ID;
  int sibling_pos = -1;
  bool use_left_sibling = false;
  
  // 优先考虑左兄弟
  if (current_pos > 0) {
    sibling_id = parent_page->ValueAt(current_pos - 1);
    sibling_pos = current_pos - 1;
    use_left_sibling = true;
  } 
  // 没有左兄弟则尝试右兄弟
  else if (current_pos < parent_page->GetSize() - 1) {
    sibling_id = parent_page->ValueAt(current_pos + 1);
    sibling_pos = current_pos + 1;
  }
  
  if (sibling_id == INVALID_PAGE_ID) {
    parent_read_guard.Drop();
    return false;
  }
  
  // 检查兄弟节点是否能提供帮助
  auto sibling_read_guard = buffer_pool_manager_->FetchPageRead(index_id_, sibling_id);
  auto sibling_page = reinterpret_cast<const BPTreePage *>(PageContentPtr(sibling_read_guard.GetData()));
  
  // 计算兄弟节点的最小安全大小
  int threshold_size;
  if (sibling_page->IsLeaf()) {
    threshold_size = sibling_page->GetMaxSize() / 2;
  } else {
    threshold_size = (sibling_page->GetMaxSize() + 1) / 2;
  }
  
  // 判断是重新分配还是合并
  bool can_redistribute = sibling_page->GetSize() > threshold_size;
  
  if (can_redistribute) {
    // 可以重新分配
    parent_read_guard.Drop();
    sibling_read_guard.Drop();
    
    // 获取写锁进行重新分配
    auto sibling_write_guard = buffer_pool_manager_->FetchPageWrite(index_id_, sibling_id);
    auto sibling_write_page = reinterpret_cast<BPTreePage *>(PageContentPtr(sibling_write_guard.GetMutableData()));
    
    auto node_write_guard = buffer_pool_manager_->FetchPageWrite(index_id_, node_id);
    auto node_write_page = reinterpret_cast<BPTreePage *>(PageContentPtr(node_write_guard.GetMutableData()));
    
    // 执行重新分配
    Redistribute(sibling_write_page, node_write_page, current_pos);
    return true;
  } else {
    // 需要合并节点
    parent_read_guard.Drop();
    sibling_read_guard.Drop();
    
    // 确定合并顺序：保证左节点在左边
    page_id_t left_page_id, right_page_id;
    int merge_position;
    
    if (use_left_sibling) {
      // 当前节点在兄弟节点右边
      left_page_id = sibling_id;
      right_page_id = node_id;
      merge_position = current_pos;
    } else {
      // 当前节点在兄弟节点左边
      left_page_id = node_id;
      right_page_id = sibling_id;
      merge_position = sibling_pos;
    }
    
    // 执行合并
    return Coalesce(left_page_id, right_page_id, parent_page_id, merge_position);
  }
}

auto BPTreeIndex::Coalesce(page_id_t neighbor_node_id, page_id_t node_id, page_id_t parent_id, int index) -> bool
{
  auto neighbor_guard = buffer_pool_manager_->FetchPageWrite(index_id_, neighbor_node_id);
  auto neighbor_page = reinterpret_cast<BPTreePage *>(PageContentPtr(neighbor_guard.GetMutableData()));
  
  auto node_guard = buffer_pool_manager_->FetchPageWrite(index_id_, node_id);
  auto node_page = reinterpret_cast<BPTreePage *>(PageContentPtr(node_guard.GetMutableData()));
  
  auto parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, parent_id);
  auto parent_page = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));
  
  if (neighbor_page->IsLeaf()) {
    // Leaf node coalesce
    auto neighbor_leaf = reinterpret_cast<BPTreeLeafPage *>(neighbor_page);
    auto node_leaf = reinterpret_cast<BPTreeLeafPage *>(node_page);
    
    // Move all records from node to neighbor
    node_leaf->MoveAllTo(neighbor_leaf);
    
    // Update sibling pointer
    if (node_leaf->GetNextPageId() != INVALID_PAGE_ID) {
      neighbor_leaf->SetNextPageId(node_leaf->GetNextPageId());
    }
  } else {
    // Internal node coalesce
    auto neighbor_internal = reinterpret_cast<BPTreeInternalPage *>(neighbor_page);
    auto node_internal = reinterpret_cast<BPTreeInternalPage *>(node_page);
    
    // Get separator key from parent
    Record middle_key(key_schema_, nullptr, parent_page->KeyAt(index), INVALID_RID);
    
    // Move all records including separator key
    node_internal->MoveAllTo(neighbor_internal, middle_key, buffer_pool_manager_);
  }
  
  // Remove entry from parent
  for (int i = index; i < parent_page->GetSize() - 1; i++) {
    parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
    parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
  }
  parent_page->SetSize(parent_page->GetSize() - 1);
  
  // Delete node
  node_guard.Drop();
  DeletePage(node_id);
  
  // Check if parent needs adjustment
  if (!parent_page->IsSafe(false) && !parent_page->IsRoot()) {
    neighbor_guard.Drop();
    parent_guard.Drop();
    CoalesceOrRedistribute(parent_id);
  } else if (parent_page->IsRoot() && parent_page->GetSize() == 1) {
        neighbor_guard.Drop();
    parent_guard.Drop();
    AdjustRoot(parent_page);
  }
  
  return true;
}

void BPTreeIndex::Redistribute(BPTreePage *neighbor_node, BPTreePage *node, int index)
{
  // 获取父节点
  auto parent_guard = buffer_pool_manager_->FetchPageWrite(index_id_, node->GetParentPageId());
  auto parent_page = reinterpret_cast<BPTreeInternalPage *>(PageContentPtr(parent_guard.GetMutableData()));
  
  // 在父节点中查找两个节点的位置
  int current_node_position = -1;
  int neighbor_node_position = -1;
  
  for (int pos = 0; pos < parent_page->GetSize(); pos++) {
    if (parent_page->ValueAt(pos) == node->GetPageId()) {
      current_node_position = pos;
    }
    if (parent_page->ValueAt(pos) == neighbor_node->GetPageId()) {
      neighbor_node_position = pos;
    }
  }
  
  // 判断邻居节点在左侧还是右侧
  bool neighbor_is_left_side = (neighbor_node_position < current_node_position);
  
  // 只处理叶子节点的情况（根据正确代码）
  if (node->IsLeaf()) {
    auto target_leaf = reinterpret_cast<BPTreeLeafPage *>(node);
    auto neighbor_leaf = reinterpret_cast<BPTreeLeafPage *>(neighbor_node);
    
    // 检查是否需要重新分配（邻居节点条目更多）
    if (neighbor_leaf->GetSize() > target_leaf->GetSize()) {
      
      if (neighbor_is_left_side) {
        // 从左侧邻居借用最后一个条目
        int last_neighbor_index = neighbor_leaf->GetSize() - 1;
        
        // 使用 Insert 方法将条目添加到目标节点
        Record moving_record(key_schema_, nullptr, 
                            neighbor_leaf->KeyAt(last_neighbor_index), 
                            INVALID_RID);
        RID moving_rid = neighbor_leaf->ValueAt(last_neighbor_index);
        
        target_leaf->Insert(moving_record, moving_rid, key_schema_);
        
        // 减少邻居节点的条目数
        neighbor_leaf->SetSize(neighbor_leaf->GetSize() - 1);
        
        // 更新父节点中的键
        parent_page->SetKeyAt(current_node_position, target_leaf->KeyAt(0));
        
      } else {
        // 从右侧邻居借用第一个条目
        // 使用 Insert 方法将条目添加到目标节点
        Record moving_record(key_schema_, nullptr, 
                            neighbor_leaf->KeyAt(0), 
                            INVALID_RID);
        RID moving_rid = neighbor_leaf->ValueAt(0);
        
        target_leaf->Insert(moving_record, moving_rid, key_schema_);
        
        // 将邻居节点的条目向左移动
        int neighbor_current_size = neighbor_leaf->GetSize();
        for (int pos = 0; pos < neighbor_current_size - 1; pos++) {
          neighbor_leaf->SetKeyAt(pos, neighbor_leaf->KeyAt(pos + 1));
          neighbor_leaf->SetValueAt(pos, neighbor_leaf->ValueAt(pos + 1));
        }
        
        // 减少邻居节点的条目数
        neighbor_leaf->SetSize(neighbor_current_size - 1);
        
        // 更新父节点中的键
        parent_page->SetKeyAt(neighbor_node_position, neighbor_leaf->KeyAt(0));
      }
    }
  } 
}

auto BPTreeIndex::AdjustRoot(BPTreePage *old_root_node) -> bool
{
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  if (old_root_node->IsLeaf()) {
    // Root is a leaf
    if (old_root_node->GetSize() == 0) {
      // Tree is empty
      header->root_page_id_ = INVALID_PAGE_ID;
      header->tree_height_ = 0;
      DeletePage(old_root_node->GetPageId());
      return true;
    }
  } else {
    // Root is an internal node
    auto internal_root = reinterpret_cast<BPTreeInternalPage *>(old_root_node);
    if (internal_root->GetSize() == 1) {
      // Root has only one child
      page_id_t new_root_id = internal_root->ValueAt(0);
      
      auto new_root_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_root_id);
      auto new_root_page = reinterpret_cast<BPTreePage *>(PageContentPtr(new_root_guard.GetMutableData()));
      new_root_page->SetParentPageId(INVALID_PAGE_ID);
      
      header->root_page_id_ = new_root_id;
      header->tree_height_--;
      
      DeletePage(old_root_node->GetPageId());
      return true;
    }
  }
  
  return false;
}


auto BPTreeIndex::Search(const Record &key) -> std::vector<RID>
{
  //printf("\n=== Starting Search: key=%s ===\n", key.GetValueAt(0)->ToString().c_str());
  
  std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  // printf("Tree state: height=%lu, root=%d, entries=%lu\n", 
  //        header->tree_height_, header->root_page_id_, header->num_entries_);
  
  // Find the leaf page that should contain the key
  page_id_t leaf_page_id = FindLeafPage(key);
  
  //printf("Found leaf page ID: %d\n", leaf_page_id);
  
  // if (leaf_page_id == INVALID_PAGE_ID) {
  //   printf("Leaf page not found, returning empty result\n");
  //   return {};
  // }
  
  auto leaf_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_page_id);
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  
  //printf("Searching in leaf page %d:\n", leaf_page_id);
  //DebugPrintLeaf(leaf_page, key_schema_);
  
  auto result = leaf_page->Lookup(key, key_schema_);
  
  //printf("Search found %zu result(s)\n", result.size());
  // for (size_t i = 0; i < result.size(); i++) {
  //   printf("  Result %zu: RID=(%d,%d)\n", i, result[i].PageID(), result[i].SlotID());
  // }
  
  //printf("=== Search completed ===\n\n");
  
  return result;
}

auto BPTreeIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  std::shared_lock lock(index_latch_);
  
  std::vector<RID> result;
  
  // Find starting leaf page
  page_id_t leaf_page_id = FindLeafPageForRange(low_key, true);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return result;
  }
  
  // Traverse leaf pages
  while (leaf_page_id != INVALID_PAGE_ID) {
    auto leaf_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_page_id);
    auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
    
    // Find range within this leaf
    int start = leaf_page->LowerBound(low_key, key_schema_);
    int end = leaf_page->UpperBound(high_key, key_schema_);
    
    // Collect RIDs in range
    for (int i = start; i < end; i++) {
      result.push_back(leaf_page->ValueAt(i));
    }
    
    // Check if we need to continue
    if (end < leaf_page->GetSize() || Record::Compare(high_key, 
        Record(key_schema_, nullptr, leaf_page->KeyAt(leaf_page->GetSize() - 1), INVALID_RID)) < 0) {
      break;
    }
    
    // Move to next leaf
    leaf_page_id = leaf_page->GetNextPageId();
  }
  
  return result;
}

// Iterator implementation
BPTreeIndex::BPTreeIterator::BPTreeIterator(BPTreeIndex *tree, page_id_t leaf_page_id, int index)
    : tree_(tree), leaf_page_id_(leaf_page_id), index_(index)
{}

auto BPTreeIndex::BPTreeIterator::IsValid() -> bool
{
  if (leaf_page_id_ == INVALID_PAGE_ID || index_ < 0) {
    return false;
  }
  
  auto leaf_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  if (!leaf_guard.IsValid()) {
    return false;
  }
  
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  return index_ < leaf_page->GetSize();
}

void BPTreeIndex::BPTreeIterator::Next()
{
  if (!IsValid()) {
    return;
  }
  
  auto leaf_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  
  index_++;
  
  if (index_ >= leaf_page->GetSize()) {
    leaf_page_id_ = leaf_page->GetNextPageId();
    index_ = 0;
  }
}

auto BPTreeIndex::BPTreeIterator::GetKey() -> Record
{
  if (!IsValid()) {
    return Record(nullptr, nullptr, nullptr, INVALID_RID);
  }
  
  auto leaf_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  
  return Record(tree_->key_schema_, nullptr, leaf_page->KeyAt(index_), INVALID_RID);
}

auto BPTreeIndex::BPTreeIterator::GetRID() -> RID
{
  if (!IsValid()) {
    return INVALID_RID;
  }
  
  auto leaf_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  
  return leaf_page->ValueAt(index_);
}

auto BPTreeIndex::Begin() -> std::unique_ptr<IIterator>
{
  std::shared_lock lock(index_latch_);

  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());

  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
  }

  page_id_t current_page_id = header->root_page_id_;

  while (true) {
    auto current_guard = buffer_pool_manager_->FetchPageRead(index_id_, current_page_id);
    auto current_page = reinterpret_cast<const BPTreePage *>(PageContentPtr(current_guard.GetData()));

    if (current_page->IsLeaf()) {
      break;
    }

    auto internal_page = reinterpret_cast<const BPTreeInternalPage *>(current_page);
    current_page_id = internal_page->ValueAt(0);
  }

  return std::make_unique<BPTreeIterator>(this, current_page_id, 0);
}

auto BPTreeIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
  }
  
  // 使用FindLeafPageForRange而不是FindLeafPage，因为它更适合范围查询
  page_id_t leaf_page_id = FindLeafPageForRange(key, true);  // true表示使用LowerBound查找
  if (leaf_page_id == INVALID_PAGE_ID) {
    return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
  }
  
  auto leaf_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_page_id);
  auto leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
  
  // 找到第一个大于等于key的位置
  int index = leaf_page->LowerBound(key, key_schema_);
  
  // 如果当前位置超出当前叶子节点，需要移动到下一个叶子节点
  while (index >= leaf_page->GetSize()) {
    leaf_page_id = leaf_page->GetNextPageId();
    if (leaf_page_id == INVALID_PAGE_ID) {
      return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
    }
    
    leaf_guard = buffer_pool_manager_->FetchPageRead(index_id_, leaf_page_id);
    leaf_page = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
    index = 0;  // 从下一个叶子节点的开头开始
  }
  
  return std::make_unique<BPTreeIterator>(this, leaf_page_id, index);
}

auto BPTreeIndex::End() -> std::unique_ptr<IIterator>
{
  return std::make_unique<BPTreeIterator>(this, INVALID_PAGE_ID, 0);
}

void BPTreeIndex::Clear()
{
  std::unique_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());
  
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  
  ClearPage(header->root_page_id_);
  
  header->root_page_id_ = INVALID_PAGE_ID;
  header->first_free_page_id_ = INVALID_PAGE_ID;
  header->tree_height_ = 0;
  header->num_entries_ = 0;
  header->page_num_ = 1;
}

void BPTreeIndex::ClearPage(page_id_t page_id)
{
  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, page_id);
  auto page = reinterpret_cast<BPTreePage *>(PageContentPtr(page_guard.GetMutableData()));
  
  if (!page->IsLeaf()) {
    // Internal node: recursively clear children
    auto internal_page = reinterpret_cast<BPTreeInternalPage *>(page);
    for (int i = 0; i < internal_page->GetSize(); i++) {
      page_id_t child_id = internal_page->ValueAt(i);
      if (child_id != INVALID_PAGE_ID) {
        ClearPage(child_id);
      }
    }
  }
  
  DeletePage(page_id);
}

auto BPTreeIndex::IsEmpty() -> bool
{
  std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  return header->num_entries_ == 0;
}

auto BPTreeIndex::Size() -> size_t
{
  std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  return header->num_entries_;
}

auto BPTreeIndex::GetHeight() -> int
{
  std::shared_lock lock(index_latch_);
  
  auto header_guard = buffer_pool_manager_->FetchPageRead(index_id_, FILE_HEADER_PAGE_ID);
  auto header = reinterpret_cast<const BPTreeIndexHeader *>(header_guard.GetData());
  
  return static_cast<int>(header->tree_height_);
}

}  // namespace njudb
