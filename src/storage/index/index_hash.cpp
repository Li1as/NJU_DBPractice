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
#include <memory>
#include <cstddef>

namespace njudb {

// HashBucketPage implementation
auto HashBucketPage::GetMaxEntries(size_t key_size) -> size_t {
  // available space for entries in the page's data area
  // Use offsetof to account for struct padding and alignment
  size_t header_size = offsetof(HashBucketPage, data_);
  size_t available   = PAGE_SIZE - PAGE_HEADER_SIZE - header_size;
  size_t entry_size  = key_size + sizeof(page_id_t) + sizeof(slot_id_t);
  return available / entry_size;
}

void HashBucketPage::WriteEntry(size_t offset, const Record &key, const RID &rid)
{
  size_t key_len = key.GetSchema()->GetRecordLength();
  size_t entry_size = key_len + sizeof(page_id_t) + sizeof(slot_id_t);
  // bounds check
  size_t max_entries = GetMaxEntries(key_len);
  NJUDB_ASSERT(offset < max_entries, "HashBucketPage::WriteEntry out of bounds");
  char *dst = data_ + offset * entry_size;
  // copy key raw bytes
  std::memcpy(dst, key.GetData(), key_len);
  dst += key_len;
  // copy RID
  *reinterpret_cast<page_id_t *>(dst) = rid.PageID();
  dst += sizeof(page_id_t);
  *reinterpret_cast<slot_id_t *>(dst) = rid.SlotID();
}

void HashBucketPage::ReadEntry(size_t offset, Record &key, RID &rid, size_t key_size) const
{
  size_t max_entries = GetMaxEntries(key_size);
  NJUDB_ASSERT(offset < max_entries, "HashBucketPage::ReadEntry out of bounds");
  size_t entry_size = key_size + sizeof(page_id_t) + sizeof(slot_id_t);
  const char *src   = data_ + offset * entry_size;
  // copy key bytes into temporary buffer
  std::unique_ptr<char[]> buf(new char[key_size]);
  std::memcpy(buf.get(), src, key_size);
  src += key_size;
  // read RID
  page_id_t pid = *reinterpret_cast<const page_id_t *>(src);
  src += sizeof(page_id_t);
  slot_id_t sid = *reinterpret_cast<const slot_id_t *>(src);
  rid = RID(pid, sid);
  // construct a temporary record from bytes then assign
  Record tmp(key.GetSchema(), nullptr, buf.get(), rid);
  key = tmp;
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
    // sanitize directory entries to avoid pointing to reserved pages
    auto directory_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
    auto directory_page  = reinterpret_cast<HashBucketDirectory *>(directory_guard.GetMutableData());
    for (size_t i = 0; i < bucket_count_; ++i) {
      if (directory_page->bucket_page_ids_[i] != INVALID_PAGE_ID && directory_page->bucket_page_ids_[i] < 2) {
        directory_page->bucket_page_ids_[i] = INVALID_PAGE_ID;
      }
    }
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
    // Too many buckets for single directory page - limit to available space
    bucket_count_              = max_bucket_refs;
    header_page->bucket_count_ = bucket_count_;
  }

  // Initialize entire directory to invalid to avoid leftover pointers
  for (size_t i = 0; i < max_bucket_refs; ++i) {
    directory_page->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }

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
  page_id_t new_page_id = INVALID_PAGE_ID;

  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header      = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
  // defensive: header->next_page_id_ should never point to reserved pages
  if (header->next_page_id_ < 2) {
    header->next_page_id_ = 2;
  }
  new_page_id      = header->next_page_id_;
  // bump next page id
  header->next_page_id_++; 

  // initialize the new page (only zero the page *content* area to avoid touching page header fields)
  auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_page_id);
  auto page_data  = page_guard.GetMutableData();
  std::memset(PageContentPtr(page_data), 0, PAGE_SIZE - PAGE_HEADER_SIZE);
  auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_data));
  bucket->next_page_id_ = INVALID_PAGE_ID;
  bucket->entry_count_  = 0;

  return new_page_id;
}

void HashIndex::Insert(const Record &key, const RID &rid)
{
  size_t bucket_index = Hash(key);
  InsertIntoBucket(bucket_index, key, rid);
  // update header total entries
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header      = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
  header->total_entries_++;
  total_entries_++;
}

void HashIndex::InsertIntoBucket(size_t bucket_index, const Record &key, const RID &rid)
{
  // locate bucket page id
  auto dir_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto dir_page  = reinterpret_cast<HashBucketDirectory *>(dir_guard.GetMutableData());
  page_id_t pid  = dir_page->bucket_page_ids_[bucket_index];

  // defensive: if directory points to reserved or invalid pages, re-allocate
  if (pid == INVALID_PAGE_ID || pid < 2) {
    pid = AllocateBucketPage();
    dir_page->bucket_page_ids_[bucket_index] = pid;
  }

  // traverse chain to find a page with space
  page_id_t cur = pid;
  while (true) {
    auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, cur);
    auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()));
    // defensive: sanitize next_page_id_ if it points to reserved pages (prevent accidental overwrite of header/dir)
    if (bucket->next_page_id_ != INVALID_PAGE_ID && bucket->next_page_id_ < 2) {
      bucket->next_page_id_ = INVALID_PAGE_ID;
    }
    size_t max_ent  = HashBucketPage::GetMaxEntries(key_size_);
    if (bucket->entry_count_ < max_ent) {
      // write at the end
      NJUDB_ASSERT(bucket->entry_count_ < max_ent, "InsertIntoBucket: bucket entry_count exceeds max");
      bucket->WriteEntry(bucket->entry_count_, key, rid);
      bucket->entry_count_++;
      break;
    }
    if (bucket->next_page_id_ != INVALID_PAGE_ID) {
      cur = bucket->next_page_id_;
      continue;
    }
    // allocate overflow page
    page_id_t new_pid = AllocateBucketPage();
    bucket->next_page_id_ = new_pid;
    // write to new page
    auto new_guard = buffer_pool_manager_->FetchPageWrite(index_id_, new_pid);
    auto new_bucket = reinterpret_cast<HashBucketPage *>(PageContentPtr(new_guard.GetMutableData()));
    NJUDB_ASSERT(new_bucket->entry_count_ < max_ent, "InsertIntoBucket: new bucket full (unexpected)");
    new_bucket->WriteEntry(new_bucket->entry_count_, key, rid);
    new_bucket->entry_count_++;
    break;
  }
}

auto HashIndex::Delete(const Record &key) -> bool
{
  size_t bucket_index = Hash(key);
  size_t deleted = DeleteAllFromBucket(bucket_index, key);
  if (deleted == 0) return false;
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header      = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
  header->total_entries_ -= deleted;
  total_entries_ -= deleted;
  return true;
}

auto HashIndex::DeleteAllFromBucket(size_t bucket_index, const Record &key) -> size_t
{
  size_t deleted = 0;
  auto dir_guard  = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto dir_page   = reinterpret_cast<HashBucketDirectory *>(dir_guard.GetMutableData());
  page_id_t pid   = dir_page->bucket_page_ids_[bucket_index];
  if (pid == INVALID_PAGE_ID) return 0;
  // defensive: directory pointer should not reference reserved pages
  if (pid < 2) {
    dir_page->bucket_page_ids_[bucket_index] = INVALID_PAGE_ID;
    return 0;
  }

  page_id_t cur = pid;
  while (cur != INVALID_PAGE_ID) {
    auto page_guard = buffer_pool_manager_->FetchPageWrite(index_id_, cur);
    auto bucket     = reinterpret_cast<HashBucketPage *>(PageContentPtr(page_guard.GetMutableData()));
    // defensive: sanitize next page id if invalid
    if (bucket->next_page_id_ != INVALID_PAGE_ID && bucket->next_page_id_ < 2) {
      bucket->next_page_id_ = INVALID_PAGE_ID;
    }
    size_t write_idx = 0;
    size_t max_ent = HashBucketPage::GetMaxEntries(key_size_);
    size_t orig = bucket->entry_count_;
    if (orig > max_ent) {
      // defensive: clamp to max_ent to avoid OOB reads
      orig = max_ent;
    }
    size_t entry_size = key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);

    for (size_t read_idx = 0; read_idx < orig; ++read_idx) {
      // read entry bytes
      const char *src = bucket->data_ + read_idx * entry_size;
      std::unique_ptr<char[]> buf(new char[key_size_]);
      std::memcpy(buf.get(), src, key_size_);
      src += key_size_;
      page_id_t pid_entry = *reinterpret_cast<const page_id_t *>(src);
      src += sizeof(page_id_t);
      slot_id_t sid_entry = *reinterpret_cast<const slot_id_t *>(src);
      RID rid_entry(pid_entry, sid_entry);
      Record rec(key_schema_, nullptr, buf.get(), rid_entry);
      if (Record::Compare(rec, key) == 0) {
        ++deleted;
        continue; // skip (i.e., delete)
      }
      // keep this entry, move to write_idx if needed
      if (write_idx != read_idx) {
        bucket->WriteEntry(write_idx, rec, rid_entry);
      }
      write_idx++;
    }
    bucket->entry_count_ = write_idx;
    // do not free empty pages for simplicity
    {
      page_id_t next_pid = bucket->next_page_id_;
      if (next_pid != INVALID_PAGE_ID && next_pid < 2) {
        // corrupted pointer, stop the chain
        break;
      }
      cur = next_pid;
    }
  }
  return deleted;
}

auto HashIndex::Search(const Record &key) -> std::vector<RID>
{
  size_t bucket_index = Hash(key);
  return SearchInBucket(bucket_index, key);
}

auto HashIndex::SearchInBucket(size_t bucket_index, const Record &key) -> std::vector<RID>
{
  std::vector<RID> result;
  auto dir_guard = buffer_pool_manager_->FetchPageRead(index_id_, HASH_KEY_PAGE);
  auto dir_page  = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());
  page_id_t pid  = dir_page->bucket_page_ids_[bucket_index];
  if (pid == INVALID_PAGE_ID) return result;
  if (pid < 2) {
    // sanitize directory and treat as empty
    auto dir_guard_w = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
    auto dir_page_w  = reinterpret_cast<HashBucketDirectory *>(dir_guard_w.GetMutableData());
    dir_page_w->bucket_page_ids_[bucket_index] = INVALID_PAGE_ID;
    return result;
  }

  page_id_t cur = pid;
  size_t entry_size = key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);
  while (cur != INVALID_PAGE_ID) {
    auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, cur);
    auto bucket     = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
    size_t max_ent = HashBucketPage::GetMaxEntries(key_size_);
    for (size_t i = 0; i < bucket->entry_count_ && i < max_ent; ++i) {
      const char *src = bucket->data_ + i * entry_size;
      std::unique_ptr<char[]> buf(new char[key_size_]);
      std::memcpy(buf.get(), src, key_size_);
      src += key_size_;
      page_id_t pid_entry = *reinterpret_cast<const page_id_t *>(src);
      src += sizeof(page_id_t);
      slot_id_t sid_entry = *reinterpret_cast<const slot_id_t *>(src);
      RID rid_entry(pid_entry, sid_entry);
      Record rec(key_schema_, nullptr, buf.get(), rid_entry);
      if (Record::Compare(rec, key) == 0) {
        result.push_back(rid_entry);
      }
    }
    {
      page_id_t next_pid = bucket->next_page_id_;
      if (next_pid != INVALID_PAGE_ID && next_pid < 2) {
        // corrupted pointer, treat as end of chain
        break;
      }
      cur = next_pid;
    }
  }
  return result;
}

auto HashIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  std::vector<RID> result;
  // scan all buckets
  for (size_t b = 0; b < bucket_count_; ++b) {
    auto dir_guard = buffer_pool_manager_->FetchPageRead(index_id_, HASH_KEY_PAGE);
    auto dir_page  = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());
    page_id_t pid  = dir_page->bucket_page_ids_[b];
    if (pid == INVALID_PAGE_ID) continue;
    if (pid < 2) {
      // sanitize directory
      auto dir_guard_w = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
      auto dir_page_w  = reinterpret_cast<HashBucketDirectory *>(dir_guard_w.GetMutableData());
      dir_page_w->bucket_page_ids_[b] = INVALID_PAGE_ID;
      continue;
    }
    page_id_t cur = pid;
    size_t entry_size = key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);
    while (cur != INVALID_PAGE_ID) {
      auto page_guard = buffer_pool_manager_->FetchPageRead(index_id_, cur);
      auto bucket     = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
      size_t max_ent = HashBucketPage::GetMaxEntries(key_size_);
      for (size_t i = 0; i < bucket->entry_count_ && i < max_ent; ++i) {
        const char *src = bucket->data_ + i * entry_size;
        std::unique_ptr<char[]> buf(new char[key_size_]);
        std::memcpy(buf.get(), src, key_size_);
        src += key_size_;
        page_id_t pid_entry = *reinterpret_cast<const page_id_t *>(src);
        src += sizeof(page_id_t);
        slot_id_t sid_entry = *reinterpret_cast<const slot_id_t *>(src);
        RID rid_entry(pid_entry, sid_entry);
        Record rec(key_schema_, nullptr, buf.get(), rid_entry);
        if (Record::Compare(rec, low_key) >= 0 && Record::Compare(rec, high_key) <= 0) {
          result.push_back(rid_entry);
        }
      }
      {
        page_id_t next_pid = bucket->next_page_id_;
        if (next_pid != INVALID_PAGE_ID && next_pid < 2) {
          break;
        }
        cur = next_pid;
      }
    }
  }
  return result;
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
  // advance entry
  ++current_entry_;
  // check current page
  if (current_page_id_ != INVALID_PAGE_ID) {
    auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
    auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
    if (current_entry_ < bucket->entry_count_) {
      return; // still valid in current page
    }
    // go to next page in chain
    if (bucket->next_page_id_ != INVALID_PAGE_ID) {
      current_page_id_ = bucket->next_page_id_;
      current_entry_ = 0;
      return;
    }
  }
  // move to next bucket
  ++current_bucket_;
  current_entry_ = 0;
  current_page_id_ = INVALID_PAGE_ID;
  if (current_bucket_ >= index_->bucket_count_) {
    is_end_ = true;
    return;
  }
  // find next valid entry starting from current_bucket_
  FindNextValidEntry();
}

void HashIndex::HashIterator::FindNextValidEntry()
{
  // start from current_bucket_ and search for any non-empty bucket
  while (current_bucket_ < index_->bucket_count_) {
    auto dir_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, HASH_KEY_PAGE);
    auto dir_page  = reinterpret_cast<const HashBucketDirectory *>(dir_guard.GetData());
    page_id_t pid  = dir_page->bucket_page_ids_[current_bucket_];
    if (pid == INVALID_PAGE_ID) {
      ++current_bucket_;
      continue;
    }
    if (pid < 2) {
      // sanitize directory and skip
      auto dir_guard_w = index_->buffer_pool_manager_->FetchPageWrite(index_->index_id_, HASH_KEY_PAGE);
      auto dir_page_w  = reinterpret_cast<HashBucketDirectory *>(dir_guard_w.GetMutableData());
      dir_page_w->bucket_page_ids_[current_bucket_] = INVALID_PAGE_ID;
      ++current_bucket_;
      continue;
    }
    // find first page in chain with entries
    page_id_t cur = pid;
    size_t entry_size = index_->key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);
    while (cur != INVALID_PAGE_ID) {
      auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, cur);
      auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));      size_t max_ent = HashBucketPage::GetMaxEntries(index_->key_size_);
      if (bucket->entry_count_ > max_ent) {
        // defensive clamp: treat as if only valid entries are up to max_ent
        if (max_ent == 0) {
          cur = bucket->next_page_id_;
          continue;
        }
      }      if (bucket->entry_count_ > 0) {
        current_page_id_ = cur;
        current_entry_ = 0;
        return;
      }
      {
        page_id_t next_pid = bucket->next_page_id_;
        if (next_pid != INVALID_PAGE_ID && next_pid < 2) {
          // corrupted pointer, stop scanning this bucket
          break;
        }
        cur = next_pid;
      }
    }
    ++current_bucket_;
  }
  // no valid entries
  is_end_ = true;
}

auto HashIndex::HashIterator::GetKey() -> Record
{
  // construct a record from the current entry
  NJUDB_ASSERT(!is_end_ && current_page_id_ != INVALID_PAGE_ID, "Iterator not valid");
  size_t entry_size = index_->key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);
  auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
  auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
  const char *src = bucket->data_ + current_entry_ * entry_size;
  std::unique_ptr<char[]> buf(new char[index_->key_size_]);
  std::memcpy(buf.get(), src, index_->key_size_);
  src += index_->key_size_;
  page_id_t pid_entry = *reinterpret_cast<const page_id_t *>(src);
  src += sizeof(page_id_t);
  slot_id_t sid_entry = *reinterpret_cast<const slot_id_t *>(src);
  RID rid_entry(pid_entry, sid_entry);
  Record rec(index_->key_schema_, nullptr, buf.get(), rid_entry);
  return rec;
}

auto HashIndex::HashIterator::GetRID() -> RID
{
  NJUDB_ASSERT(!is_end_ && current_page_id_ != INVALID_PAGE_ID, "Iterator not valid");
  size_t entry_size = index_->key_size_ + sizeof(page_id_t) + sizeof(slot_id_t);
  auto page_guard = index_->buffer_pool_manager_->FetchPageRead(index_->index_id_, current_page_id_);
  auto bucket = reinterpret_cast<const HashBucketPage *>(PageContentPtr(page_guard.GetData()));
  const char *src = bucket->data_ + current_entry_ * entry_size + index_->key_size_;
  page_id_t pid_entry = *reinterpret_cast<const page_id_t *>(src);
  src += sizeof(page_id_t);
  slot_id_t sid_entry = *reinterpret_cast<const slot_id_t *>(src);
  return RID(pid_entry, sid_entry);
}

auto HashIndex::Begin() -> std::unique_ptr<IIterator>
{
  return std::make_unique<HashIterator>(this, false);
}

auto HashIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  auto iter = std::make_unique<HashIterator>(this, false);
  iter->current_bucket_ = Hash(key);
  iter->current_entry_ = 0;
  iter->current_page_id_ = INVALID_PAGE_ID;
  iter->is_end_ = false;
  iter->FindNextValidEntry();
  return iter;
}

auto HashIndex::End() -> std::unique_ptr<IIterator> { return std::make_unique<HashIterator>(this, true); }

void HashIndex::Clear()
{
  // reset header
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header      = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());
  header->total_entries_ = 0;
  header->next_page_id_  = 2;
  total_entries_ = 0;
  // reset directory entries
  auto dir_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto dir_page  = reinterpret_cast<HashBucketDirectory *>(dir_guard.GetMutableData());
  for (size_t i = 0; i < bucket_count_; ++i) {
    dir_page->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto HashIndex::IsEmpty() -> bool { return total_entries_ == 0; }

auto HashIndex::Size() -> size_t { return total_entries_; }

auto HashIndex::GetHeight() -> int
{
  // Hash indexes have a constant height of 2 (header + bucket pages)
  return 2;
}

}  // namespace njudb
