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

#include "index_hash.h"
#include "../buffer/page_guard.h"
#include <algorithm>
#include <functional>
#include <cstring>
#include <stdexcept>

namespace njudb {

// HashBucketPage implementation
auto HashBucketPage::GetMaxEntries(size_t key_size) -> size_t {
  size_t header_size = sizeof(HashBucketPage);
  size_t available   = PAGE_SIZE - PAGE_HEADER_SIZE - header_size;
  size_t entry_size  = key_size + sizeof(RID);
  return available / entry_size;
}

void HashBucketPage::WriteEntry(size_t offset, const Record &key, const RID &rid)
{
  size_t key_size   = key.GetSchema()->GetRecordLength();
  size_t entry_size = key_size + sizeof(RID);
  auto   ptr        = reinterpret_cast<char *>(data_) + offset * entry_size;
  std::memcpy(ptr, key.GetData(), key_size);
  ptr += key_size;
  // write RID as page_id_t and slot_id_t
  page_id_t pid = rid.PageID();
  slot_id_t sid = rid.SlotID();
  std::memcpy(ptr, &pid, sizeof(page_id_t));
  std::memcpy(ptr + sizeof(page_id_t), &sid, sizeof(slot_id_t));
}


void HashBucketPage::ReadEntry(size_t offset, Record &key, RID &rid, size_t key_size) const
{
  size_t entry_size = key_size + sizeof(RID);
  const char *ptr   = reinterpret_cast<const char *>(data_) + offset * entry_size;
  // read key
  char *keybuf = new char[key_size];
  std::memcpy(keybuf, ptr, key_size);
  ptr += key_size;
  // read RID
  page_id_t pid;
  slot_id_t sid;
  std::memcpy(&pid, ptr, sizeof(page_id_t));
  std::memcpy(&sid, ptr + sizeof(page_id_t), sizeof(slot_id_t));
  rid = RID(pid, sid);
  // construct a temporary Record and assign
  Record tmp(key.GetSchema(), nullptr, keybuf, rid);
  key = tmp;
  delete[] keybuf;
}


// HashIndex implementation
HashIndex::HashIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::HASH, index_id, key_schema),
      bucket_count_(DEFAULT_BUCKET_COUNT),
      total_entries_(0),
      key_size_(key_schema->GetRecordLength())  // Only key data, no null map
{
  InitializeHashIndex();
}

void HashIndex::InitializeHashIndex()
{
  // Create header page for hash index
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header_page  = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());

  if (header_page->bucket_count_ != 0) {
    // It is an index that has already been initialized
    bucket_count_  = header_page->bucket_count_;
    total_entries_ = header_page->total_entries_;
    return;
  }

  // Initialize header page
  header_page->bucket_count_  = bucket_count_;
  header_page->total_entries_ = 0;
  header_page->next_page_id_  = 2;  // Start allocating from page 2 (skip header and directory)

  // Initialize bucket directory page
  auto directory_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto directory_page  = reinterpret_cast<HashBucketDirectory *>(directory_guard.GetMutableData());

  // Calculate how many bucket page IDs can fit in the directory page
  size_t available_space = PAGE_SIZE - sizeof(HashBucketDirectory) - PAGE_HEADER_SIZE;
  size_t max_bucket_refs = available_space / sizeof(page_id_t);

  if (bucket_count_ > max_bucket_refs) {
    // Too many buckets for single directory page - should handle this case
    // For now, limit to available space
    bucket_count_              = max_bucket_refs;
    header_page->bucket_count_ = bucket_count_;
  }

  // Initialize bucket page IDs - initially all invalid
  for (size_t i = 0; i < bucket_count_; ++i) {
    directory_page->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }

  // Persist the initialized metadata immediately to avoid losing index structure
  buffer_pool_manager_->FlushPage(index_id_, FILE_HEADER_PAGE_ID);
  buffer_pool_manager_->FlushPage(index_id_, HASH_KEY_PAGE);

  // header_guard and directory_guard will automatically unpin the pages as dirty when they go out of scope
}

auto HashIndex::Hash(const Record &key) -> size_t
{
  // Use the Record's built-in hash function
  return key.Hash() % bucket_count_;
}

auto HashIndex::GetHashHeaderPage() -> HashHeaderPage *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, FILE_HEADER_PAGE_ID);
  return reinterpret_cast<HashHeaderPage *>(page->GetData());
}

auto HashIndex::GetBucketDirectory() -> HashBucketDirectory *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, HASH_KEY_PAGE);
  return reinterpret_cast<HashBucketDirectory *>(page->GetData());
}

auto HashIndex::GetBucketPage(page_id_t page_id) -> HashBucketPage *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, page_id);
  return reinterpret_cast<HashBucketPage *>(PageContentPtr(page->GetData()));
}

auto HashIndex::AllocateBucketPage() -> page_id_t
{
  // Allocate a new bucket page
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header_page  = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());

  page_id_t new_page_id = header_page->next_page_id_++;
  buffer_pool_manager_->FlushPage(index_id_, FILE_HEADER_PAGE_ID);

  // initialize the page
  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_page_id);
  auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()));
  bucket->next_page_id_ = INVALID_PAGE_ID;
  bucket->entry_count_  = 0;
  // clear data area
  size_t data_size = PAGE_SIZE - PAGE_HEADER_SIZE - sizeof(HashBucketPage);
  std::memset(bucket->data_, 0, data_size);

  return new_page_id;
}

void HashIndex::Insert(const Record &key, const RID &rid)
{
  size_t bucket_idx = Hash(key);
  InsertIntoBucket(bucket_idx, key, rid);
  // update header
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header_page  = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
  header_page->total_entries_ += 1;
  total_entries_ = header_page->total_entries_;
  buffer_pool_manager_->FlushPage(index_id_, FILE_HEADER_PAGE_ID);
}


void HashIndex::InsertIntoBucket(size_t bucket_index, const Record &key, const RID &rid)
{
  auto directory_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto directory      = reinterpret_cast<HashBucketDirectory *>(directory_guard.GetMutableData());

  page_id_t page_id = directory->bucket_page_ids_[bucket_index];
  if (page_id == INVALID_PAGE_ID) {
    // allocate first bucket page
    page_id = AllocateBucketPage();
    directory->bucket_page_ids_[bucket_index] = page_id;
    buffer_pool_manager_->FlushPage(index_id_, HASH_KEY_PAGE);
  }

  // find page with available space
  page_id_t cur_page_id = page_id;
  while (true) {
    auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, cur_page_id);
    auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()));
    size_t max_ent  = HashBucketPage::GetMaxEntries(key_size_);
    if (bucket->entry_count_ < max_ent) {
      // append
      bucket->WriteEntry(bucket->entry_count_, key, rid);
      bucket->entry_count_++;
      buffer_pool_manager_->FlushPage(index_id_, cur_page_id);
      return;
    } else {
      if (bucket->next_page_id_ == INVALID_PAGE_ID) {
        page_id_t new_page = AllocateBucketPage();
        bucket->next_page_id_ = new_page;
        buffer_pool_manager_->FlushPage(index_id_, cur_page_id);
        // move to new page
        cur_page_id = new_page;
      } else {
        cur_page_id = bucket->next_page_id_;
      }
    }
  }
}


auto HashIndex::Delete(const Record &key) -> bool
{
  size_t bucket_idx = Hash(key);
  size_t deleted    = DeleteAllFromBucket(bucket_idx, key);
  if (deleted > 0) {
    auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
    auto header_page  = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
    header_page->total_entries_ -= deleted;
    total_entries_ = header_page->total_entries_;
    buffer_pool_manager_->FlushPage(index_id_, FILE_HEADER_PAGE_ID);
    return true;
  }
  return false;
}


auto HashIndex::DeleteAllFromBucket(size_t bucket_index, const Record &key) -> size_t
{
  size_t deleted = 0;
  auto dir_guard  = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto directory  = reinterpret_cast<HashBucketDirectory *>(dir_guard.GetMutableData());

  page_id_t *bucket_ref = &directory->bucket_page_ids_[bucket_index];
  page_id_t cur_page_id = *bucket_ref;
  page_id_t prev_page_id = INVALID_PAGE_ID;

  size_t entry_size = key_size_ + sizeof(RID);

  while (cur_page_id != INVALID_PAGE_ID) {
    auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, cur_page_id);
    auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()));
    size_t i = 0;
    while (i < bucket->entry_count_) {
      char *entry_ptr = reinterpret_cast<char *>(bucket->data_) + i * entry_size;
      if (std::memcmp(entry_ptr, key.GetData(), key_size_) == 0) {
        // remove this entry by shifting left
        char *src = entry_ptr + entry_size;
        size_t move_bytes = (bucket->entry_count_ - i - 1) * entry_size;
        if (move_bytes > 0) {
          std::memmove(entry_ptr, src, move_bytes);
        }
        bucket->entry_count_--;
        deleted++;
        buffer_pool_manager_->FlushPage(index_id_, cur_page_id);
        // continue without incrementing i to check the new occupant
      } else {
        ++i;
      }
    }

    // if page becomes empty, release it (if it's not the first page) or if first page empty and no next, release it
    if (bucket->entry_count_ == 0) {
      page_id_t next = bucket->next_page_id_;
      // release this page guard before deleting
      page_guard.Drop();
      // safety: do not delete reserved pages (header, directory)
      if (cur_page_id < HASH_KEY_PAGE + 1) {
        // should not happen; just unlink without deleting to avoid corrupting file
        if (prev_page_id == INVALID_PAGE_ID) {
          *bucket_ref = next;
        } else {
          auto prev_guard = buffer_pool_manager_->FetchPageWrite(index_id_, prev_page_id);
          auto prev_bucket = reinterpret_cast<HashBucketPage *>(PageContentPtr(prev_guard.GetMutableData()));
          prev_bucket->next_page_id_ = next;
        }
        cur_page_id = next;
        continue;
      }
      // delete this page
      buffer_pool_manager_->DeletePage(index_id_, cur_page_id);
      if (prev_page_id == INVALID_PAGE_ID) {
        // first page
        *bucket_ref = next;
        buffer_pool_manager_->FlushPage(index_id_, HASH_KEY_PAGE);
      } else {
        // update prev page's next pointer
        auto prev_guard = buffer_pool_manager_->FetchPageWrite(index_id_, prev_page_id);
        auto prev_bucket = reinterpret_cast<HashBucketPage *>(PageContentPtr(prev_guard.GetMutableData()));
        prev_bucket->next_page_id_ = next;
        buffer_pool_manager_->FlushPage(index_id_, prev_page_id);
      }
      cur_page_id = next;
      // do not update prev_page_id when deleting current page
    } else {
      // move to next
      prev_page_id = cur_page_id;
      cur_page_id = bucket->next_page_id_;
    }
  }

  return deleted;
}


auto HashIndex::Search(const Record &key) -> std::vector<RID>
{
  size_t bucket_idx = Hash(key);
  return SearchInBucket(bucket_idx, key);
}


auto HashIndex::SearchInBucket(size_t bucket_index, const Record &key) -> std::vector<RID>
{
  std::vector<RID> results;
  auto dir_guard = buffer_pool_manager_->FetchPageRead(index_id_, HASH_KEY_PAGE);
  auto directory = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());
  page_id_t cur   = directory->bucket_page_ids_[bucket_index];
  size_t entry_size = key_size_ + sizeof(RID);
  while (cur != INVALID_PAGE_ID) {
    auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, cur);
    auto bucket     = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
    for (size_t i = 0; i < bucket->entry_count_; ++i) {
      const char *entry_ptr = reinterpret_cast<const char *>(bucket->data_) + i * entry_size;
      if (std::memcmp(entry_ptr, key.GetData(), key_size_) == 0) {
        page_id_t pid;
        slot_id_t sid;
        std::memcpy(&pid, entry_ptr + key_size_, sizeof(page_id_t));
        std::memcpy(&sid, entry_ptr + key_size_ + sizeof(page_id_t), sizeof(slot_id_t));
        results.emplace_back(pid, sid);
      }
    }
    cur = bucket->next_page_id_;
  }
  return results;
}

auto HashIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  std::vector<RID> results;
  auto dir_guard = buffer_pool_manager_->FetchPageRead(index_id_, HASH_KEY_PAGE);
  auto directory = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());
  size_t entry_size = key_size_ + sizeof(RID);
  for (size_t b = 0; b < bucket_count_; ++b) {
    page_id_t cur = directory->bucket_page_ids_[b];
    while (cur != INVALID_PAGE_ID) {
      auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, cur);
      auto bucket     = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
      for (size_t i = 0; i < bucket->entry_count_; ++i) {
        size_t key_len = GetKeySchema()->GetRecordLength();
        const char *entry_ptr = reinterpret_cast<const char *>(bucket->data_) + i * (key_len + sizeof(RID));
        // build a Record to compare
        char *keybuf = new char[key_len];
        std::memcpy(keybuf, entry_ptr, key_len);
        page_id_t pid;
        slot_id_t sid;
        std::memcpy(&pid, entry_ptr + key_len, sizeof(page_id_t));
        std::memcpy(&sid, entry_ptr + key_len + sizeof(page_id_t), sizeof(slot_id_t));
        RID rid(pid, sid);
        Record rec(low_key.GetSchema(), nullptr, keybuf, rid);
        delete[] keybuf;
        if (Record::Compare(rec, low_key) >= 0 && Record::Compare(rec, high_key) <= 0) {
          results.push_back(rid);
        }
      }
      cur = bucket->next_page_id_;
    }
  }
  return results;
}

// HashIterator implementation
HashIndex::HashIterator::HashIterator(HashIndex *index, bool is_end)
    : index_(index), current_bucket_(0), current_entry_(0), current_page_id_(INVALID_PAGE_ID), is_end_(is_end)
{
  if (!is_end_) {
    FindNextValidEntry();
  }
}

auto HashIndex::HashIterator::IsValid() -> bool { return !is_end_ && current_bucket_ < index_->bucket_count_; }

void HashIndex::HashIterator::Next()
{
  if (is_end_) return;

  // try move within current page
  if (current_page_id_ != INVALID_PAGE_ID) {
    auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
    auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
    if (current_entry_ + 1 < bucket->entry_count_) {
      ++current_entry_;
      return;
    }
    // try move to next overflow page
    if (bucket->next_page_id_ != INVALID_PAGE_ID) {
      current_page_id_ = bucket->next_page_id_;
      current_entry_ = 0;
      return;
    }
  }

  // move to next bucket
  ++current_bucket_;
  FindNextValidEntry();
}


void HashIndex::HashIterator::FindNextValidEntry()
{
  auto dir_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, HASH_KEY_PAGE);
  auto directory = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());

  while (current_bucket_ < index_->bucket_count_) {
    page_id_t pid = directory->bucket_page_ids_[current_bucket_];
    page_id_t cur = pid;
    while (cur != INVALID_PAGE_ID) {
      auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, cur);
      auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
      if (bucket->entry_count_ > 0) {
        current_page_id_ = cur;
        current_entry_ = 0;
        return;
      }
      cur = bucket->next_page_id_;
    }
    ++current_bucket_;
  }
  // no more entries
  is_end_ = true;
}


auto HashIndex::HashIterator::GetKey() -> Record
{
  if (is_end_) NJUDB_FATAL("Iterator out of range");
  auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
  auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
  size_t key_len = index_->GetKeySchema()->GetRecordLength();
  size_t entry_size = key_len + sizeof(RID);
  const char *entry_ptr = reinterpret_cast<const char *>(bucket->data_) + current_entry_ * entry_size;
  char *keybuf = new char[key_len];
  std::memcpy(keybuf, entry_ptr, key_len);
  page_id_t pid;
  slot_id_t sid;
  std::memcpy(&pid, entry_ptr + key_len, sizeof(page_id_t));
  std::memcpy(&sid, entry_ptr + key_len + sizeof(page_id_t), sizeof(slot_id_t));
  RID rid(pid, sid);
  Record rec(index_->GetKeySchema(), nullptr, keybuf, rid);
  delete[] keybuf;
  return rec;
}

auto HashIndex::HashIterator::GetRID() -> RID
{
  if (is_end_) NJUDB_FATAL("Iterator out of range");
  auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
  auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
  size_t key_len = index_->GetKeySchema()->GetRecordLength();
  size_t entry_size = key_len + sizeof(RID);
  const char *entry_ptr = reinterpret_cast<const char *>(bucket->data_) + current_entry_ * entry_size;
  page_id_t pid;
  slot_id_t sid;
  std::memcpy(&pid, entry_ptr + key_len, sizeof(page_id_t));
  std::memcpy(&sid, entry_ptr + key_len + sizeof(page_id_t), sizeof(slot_id_t));
  return RID(pid, sid);
}

auto HashIndex::Begin() -> std::unique_ptr<IIterator>
{
  // start from first non-empty bucket
  auto it = std::make_unique<HashIterator>(this, false);
  return it;
}

auto HashIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  // For hash index, we can start from the bucket containing the key
  auto it = std::make_unique<HashIterator>(this, false);
  // move iterator to the bucket containing the provided key if possible
  size_t bucket_idx = Hash(key);
  it->current_bucket_ = bucket_idx;
  it->FindNextValidEntry();
  return it;
}

auto HashIndex::End() -> std::unique_ptr<IIterator> { return std::make_unique<HashIterator>(this, true); }

void HashIndex::Clear()
{
  auto dir_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto directory = reinterpret_cast<HashBucketDirectory *>(dir_guard.GetMutableData());
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header_page = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());

  // delete all bucket pages
  for (size_t b = 0; b < bucket_count_; ++b) {
    page_id_t cur = directory->bucket_page_ids_[b];
    while (cur != INVALID_PAGE_ID) {
      auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, cur);
      page_id_t next = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()))->next_page_id_;
      // release before deleting
      page_guard.Drop();
      if (cur < HASH_KEY_PAGE + 1) {
        // never delete header or directory
        cur = next;
        continue;
      }
      buffer_pool_manager_->DeletePage(index_id_, cur);
      cur = next;
    }
    directory->bucket_page_ids_[b] = INVALID_PAGE_ID;
  }

  header_page->total_entries_ = 0;
  header_page->next_page_id_ = 2; // reset allocator
  total_entries_ = 0;

  buffer_pool_manager_->FlushPage(index_id_, HASH_KEY_PAGE);
  buffer_pool_manager_->FlushPage(index_id_, FILE_HEADER_PAGE_ID);
}


auto HashIndex::IsEmpty() -> bool { return total_entries_ == 0; }

auto HashIndex::Size() -> size_t { return total_entries_; }

auto HashIndex::GetHeight() -> int
{
  // Hash indexes have a constant height of 2 (header + bucket pages)
  return 2;
}

}  // namespace njudb
