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
// Created by ziqi on 2024/7/18.
//

#include "executor_idxscan.h"
#include "common/value.h"
#include "expr/condition_expr.h"
#include <algorithm>

namespace njudb {

IdxScanExecutor::IdxScanExecutor(TableHandle *tbl, IndexHandle *idx, ConditionVec conds, bool is_ascending)
    : AbstractExecutor(Basic), tbl_(tbl), idx_(idx), conds_(std::move(conds)), low_(nullptr), high_(nullptr), is_ascending_(is_ascending),
      needs_first_record_check_(false), needs_last_record_check_(false), start_idx_(0), end_idx_(0), current_idx_(0)
{
  // no owned out_schema_, GetOutSchema() returns tbl_->GetSchema()
}

void IdxScanExecutor::Init()
{
  // generate low/high keys according to index conditions and search the index
  GenerateRangeKeys();

  // perform index range search
  if (low_ == nullptr || high_ == nullptr) {
    rids_.clear();
  } else {
    rids_ = idx_->SearchRange(*low_, *high_);
  }

  // if descending, reverse the result vector to iterate in requested order
  if (!is_ascending_) {
    std::reverse(rids_.begin(), rids_.end());
  }

  // set range boundaries
  start_idx_ = 0;
  end_idx_   = rids_.size();

  // if low bound is exclusive (OP_GT), skip first if equal
  if (needs_first_record_check_ && start_idx_ < end_idx_) {
    auto rec = tbl_->GetRecord(rids_[start_idx_]);
    Record key_rec(&idx_->GetKeySchema(), *rec);
    if (Record::Compare(key_rec, *low_) == 0) {
      ++start_idx_;
    }
  }

  // if high bound is exclusive (OP_LT), skip last if equal
  if (needs_last_record_check_ && start_idx_ < end_idx_) {
    // last index is end_idx_ - 1
    size_t last = end_idx_ - 1;
    auto rec    = tbl_->GetRecord(rids_[last]);
    Record key_rec(&idx_->GetKeySchema(), *rec);
    if (Record::Compare(key_rec, *high_) == 0) {
      --end_idx_;
    }
  }

  current_idx_ = start_idx_;

  // initialize record_ with first element if any
  if (!IsEnd()) {
    auto rec = tbl_->GetRecord(rids_[current_idx_]);
    record_   = std::make_unique<Record>(*rec);
    ++current_idx_;
  } else {
    record_.reset();
  }
}

void IdxScanExecutor::Next()
{
  if (IsEnd()) {
    record_.reset();
    return;
  }
  auto rec = tbl_->GetRecord(rids_[current_idx_]);
  record_   = std::make_unique<Record>(*rec);
  ++current_idx_;
}

auto IdxScanExecutor::IsEnd() const -> bool { return current_idx_ >= end_idx_; }

void IdxScanExecutor::GenerateRangeKeys()
{
  // prepare default min/max values for each key field
  const auto &key_schema = idx_->GetKeySchema();
  size_t key_field_count = key_schema.GetFieldCount();
  std::vector<ValueSptr> low_vals(key_field_count), high_vals(key_field_count);
  for (size_t i = 0; i < key_field_count; ++i) {
    const auto &field = key_schema.GetFieldAt(i);
    low_vals[i]  = ValueFactory::CreateMinValueForType(field.field_.field_type_);
    high_vals[i] = ValueFactory::CreateMaxValueForType(field.field_.field_type_);
  }

  // process conditions in prefix order: stop at first non-equality range/unsupported operator
  for (size_t i = 0; i < key_field_count; ++i) {
    const auto &rtfield = key_schema.GetFieldAt(i);
    // find condition whose lhs is this field
    auto it = std::find_if(conds_.begin(), conds_.end(), [&](const Condition &c) { return c.GetLCol() == rtfield; });
    if (it == conds_.end()) {
      // no more conditions on further fields
      break;
    }
    const auto &cond = *it;
    if (cond.GetRhsType() != kValue) {
      // can't use column/subquery rhs for range bounds
      break;
    }
    auto rval = cond.GetRVal();
    // cast rhs to field type if necessary
    auto casted = ValueFactory::CastTo(rval, rtfield.field_.field_type_);

    switch (cond.GetOp()) {
      case OP_EQ: {
        low_vals[i]  = casted;
        high_vals[i] = casted;
        break;
      }
      case OP_GT: {
        low_vals[i] = casted;
        // exclude equals
        needs_first_record_check_ = true;
        // further fields cannot be used
        goto finish_prefix;
      }
      case OP_GE: {
        low_vals[i] = casted;
        // inclusive, no extra check needed
        goto finish_prefix;
      }
      case OP_LT: {
        high_vals[i] = casted;
        needs_last_record_check_ = true;
        goto finish_prefix;
      }
      case OP_LE: {
        high_vals[i] = casted;
        goto finish_prefix;
      }
      default: {
        // unsupported operator for index usage
        goto finish_prefix;
      }
    }
  }
finish_prefix:
  // construct records
  low_  = std::make_unique<Record>(&key_schema, low_vals, INVALID_RID);
  high_ = std::make_unique<Record>(&key_schema, high_vals, INVALID_RID);
}

auto IdxScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tbl_->GetSchema(); }

}  // namespace njudb
