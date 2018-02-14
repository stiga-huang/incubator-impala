// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/hdfs-orc-scanner.h"

#include <queue>

#include "exec/scanner-context.inline.h"
#include "exprs/expr.h"
#include "runtime/collection-value-builder.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/tuple-row.h"
#include "runtime/scoped-buffer.h"
#include "util/bitmap.h"
#include "util/decompress.h"
#include "util/dict-encoding.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

DEFINE_double(orc_min_filter_reject_ratio, 0.1, "(Advanced) If the percentage of "
    "rows rejected by a runtime filter drops below this value, the filter is disabled.");

// The number of row batches between checks to see if a filter is effective, and
// should be disabled. Must be a power of two.
constexpr int BATCHES_PER_FILTER_SELECTIVITY_CHECK = 16;
static_assert(BitUtil::IsPowerOf2(BATCHES_PER_FILTER_SELECTIVITY_CHECK),
    "BATCHES_PER_FILTER_SELECTIVITY_CHECK must be a power of two");

const int64_t HdfsOrcScanner::FOOTER_SIZE = 100 * 1024;

Status HdfsOrcScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const std::vector<HdfsFileDesc*> &files) {
  vector<ScanRange*> footer_ranges;
  for (int i = 0; i < files.size(); ++i) {
    // If the file size is less than 10 bytes, it is an invalid ORC file.
    if (files[i]->file_length < 10) {
      return Status(Substitute("ORC file $0 has an invalid file length: $1",
                               files[i]->filename, files[i]->file_length));
    }
    // Compute the offset of the file footer.
    int64_t footer_size = min(FOOTER_SIZE, files[i]->file_length);
    int64_t footer_start = files[i]->file_length - footer_size;
    DCHECK_GE(footer_start, 0);

    // Try to find the split with the footer.
    ScanRange* footer_split = FindFooterSplit(files[i]);

    for (int j = 0; j < files[i]->splits.size(); ++j) {
      ScanRange* split = files[i]->splits[j];

      DCHECK_LE(split->offset() + split->len(), files[i]->file_length);
      // If there are no materialized slots (such as count(*) over the table), we can
      // get the result with the file metadata alone and don't need to read any stripes
      // We only want a single node to process the file footer in this case, which is
      // the node with the footer split.  If it's not a count(*), we create a footer
      // range for the split always.
      if (!scan_node->IsZeroSlotTableScan() || footer_split == split) {
        ScanRangeMetadata* split_metadata =
            static_cast<ScanRangeMetadata*>(split->meta_data());
        // Each split is processed by first issuing a scan range for the file footer, which
        // is done here, followed by scan ranges for the columns of each stripe within the
        // actual split (in InitColumns()). The original split is stored in the metadata
        // associated with the footer range.
        ScanRange* footer_range;
        if (footer_split != nullptr) {
          footer_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), footer_size, footer_start,
              split_metadata->partition_id, footer_split->disk_id(),
              footer_split->expected_local(),
              BufferOpts(footer_split->try_cache(), files[i]->mtime), split);
        } else {
          // If we did not find the last split, we know it is going to be a remote read.
          footer_range =
              scan_node->AllocateScanRange(files[i]->fs, files[i]->filename.c_str(),
                  footer_size, footer_start, split_metadata->partition_id, -1, false,
                  BufferOpts::Uncached(), split);
        }

        footer_ranges.push_back(footer_range);
      } else {
        scan_node->RangeComplete(THdfsFileFormat::ORC, THdfsCompression::NONE);
      }
    }
  }
  // The threads that process the footer will also do the scan, so we mark all the files
  // as complete here.
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges, files.size()));
  return Status::OK();
}

ScanRange* HdfsOrcScanner::FindFooterSplit(HdfsFileDesc* file) {
  DCHECK(file != nullptr);
  for (int i = 0; i < file->splits.size(); ++i) {
    ScanRange* split = file->splits[i];
    if (split->offset() + split->len() == file->file_length) return split;
  }
  return nullptr;
}

namespace impala {

HdfsOrcScanner::OrcMemPool::OrcMemPool(MemTracker* mem_tracker) {
  mem_tracker_.reset(new MemTracker(-1, "OrcReader", mem_tracker));
}

HdfsOrcScanner::OrcMemPool::~OrcMemPool() {
  Clear();
  // We should unregister it from its parent since this is a scoped_ptr which will
  // destruct this MemTracker after the scanner is deconstructed. If its parent
  // still keep the reference, it will be used in later code path like LogUsage and
  // cause errors.
  mem_tracker_->CloseAndUnregisterFromParent();
}

void HdfsOrcScanner::OrcMemPool::Clear() {
  int64_t total_bytes_released = 0;
  for (auto it = chunk_sizes_.begin(); it != chunk_sizes_.end(); ++it) {
    std::free(it->first);
    total_bytes_released += it->second;
  }
  mem_tracker_->Release(total_bytes_released);
  chunk_sizes_.clear();
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(-total_bytes_released);
  }
  DCHECK_EQ(mem_tracker_->consumption(), 0);
}

char* HdfsOrcScanner::OrcMemPool::malloc(uint64_t size) {
  mem_tracker_->Consume(size);
  char* addr = static_cast<char*>(std::malloc(size));
  if (addr == nullptr) {
    // orc-reader does not check the result. We should throw an exception if we can't
    // malloc to stop the reader.
    mem_tracker_->Release(size);
    throw std::runtime_error("memory limit exceeded");
  }
  chunk_sizes_[addr] = size;
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(size);
  }
  return addr;
}

void HdfsOrcScanner::OrcMemPool::free(char* p) {
  DCHECK(chunk_sizes_.find(p) != chunk_sizes_.end()) << "invalid free!" << endl
       << GetStackTrace();
  std::free(p);
  int64_t size = chunk_sizes_[p];
  mem_tracker_->Release(size);
  if (ImpaladMetrics::MEM_POOL_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::MEM_POOL_TOTAL_BYTES->Increment(-size);
  }
  chunk_sizes_.erase(p);
}

void HdfsOrcScanner::ScanRangeInputStream::read(void* buf, uint64_t length,
    uint64_t offset) {
  const ScanRange* metadata_range = scanner_->metadata_range_;
  const ScanRange* split_range =
      reinterpret_cast<ScanRangeMetadata*>(metadata_range->meta_data())->original_split;
  const char* filename = scanner_->filename();
  bool expected_local = split_range->expected_local()
                   && offset >= split_range->offset()
                   && offset + length <= split_range->offset() + split_range->len();
  int64_t partition_id = scanner_->context_->partition_descriptor()->id();

  ScanRange* range = scanner_->scan_node_->AllocateScanRange(
      metadata_range->fs(), filename, length, offset, partition_id,
      split_range->disk_id(), expected_local,
      BufferOpts::ReadInto(reinterpret_cast<uint8_t*>(buf), length));

  unique_ptr<BufferDescriptor> io_buffer;
  Status status;
  {
    SCOPED_TIMER(scanner_->state_->total_storage_wait_timer());
    status = scanner_->state_->io_mgr()->Read(
        scanner_->scan_node_->reader_context(), range, &io_buffer);
  }
  if (io_buffer != nullptr) scanner_->state_->io_mgr()->ReturnBuffer(move(io_buffer));
  if (!status.ok()) {
    VLOG_QUERY << "Stop reading " << filename_ << " (offset=" << offset << ", length="
               << length << "): " << status.GetDetail();
    throw std::runtime_error("Cannot read from file: " + status.GetDetail());
  }
}

HdfsOrcScanner::HdfsOrcScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state),
      assemble_rows_timer_(scan_node_->materialize_tuple_timer()) {
  assemble_rows_timer_.Stop();
}

HdfsOrcScanner::~HdfsOrcScanner() {
}

Status HdfsOrcScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  metadata_range_ = stream_->scan_range();
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TUnit::UNIT);
  num_stats_filtered_stripes_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStatsFilteredStripes", TUnit::UNIT);
  num_stripes_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumStripes", TUnit::UNIT);
  num_scanners_with_no_reads_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumScannersWithNoReads", TUnit::UNIT);
  process_footer_timer_stats_ =
      ADD_SUMMARY_STATS_TIMER(scan_node_->runtime_profile(), "FooterProcessingTime");

  scan_node_->IncNumScannersCodegenDisabled();

  for (int i = 0; i < context->filter_ctxs().size(); ++i) {
    const FilterContext* ctx = &context->filter_ctxs()[i];
    DCHECK(ctx->filter != nullptr);
    filter_ctxs_.push_back(ctx);
  }
  filter_stats_.resize(filter_ctxs_.size());

  DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();

  reader_mem_pool_.reset(new OrcMemPool(scan_node_->mem_tracker()));
  reader_options_.setMemoryPool(*reader_mem_pool_);

  // Each scan node can process multiple splits. Each split processes the footer once.
  // We use a timer to measure the time taken to ProcessFileTail() per split and add
  // this time to the averaged timer.
  MonotonicStopWatch single_footer_process_timer;
  single_footer_process_timer.Start();
  // First process the file metadata in the footer.
  Status footer_status = ProcessFileTail();
  single_footer_process_timer.Stop();

  process_footer_timer_stats_->UpdateCounter(single_footer_process_timer.ElapsedTime());

  // Release I/O buffers immediately to make sure they are cleaned up
  // in case we return a non-OK status anywhere below.
  context_->ReleaseCompletedResources(true);
  RETURN_IF_ERROR(footer_status);

  // Update orc reader options base on the tuple descriptor
  SelectColumns(scan_node_->tuple_desc());

  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];

  return Status::OK();
}

void HdfsOrcScanner::SelectColumns(const TupleDescriptor *tuple_desc) {
  list<uint64_t> selected_indices;
  int num_columns = 0;
  // TODO validate columns. e.g. scale of decimal type
  for (SlotDescriptor* slot_desc: tuple_desc->slots()) {
    // Skip partition columns
    if (slot_desc->col_pos() < scan_node_->num_partition_keys()) continue;

    const SchemaPath &path = slot_desc->col_path();
    int col_idx = path[0];
    // The first index in a path includes the table's partition keys
    int col_idx_in_file = col_idx - scan_node_->num_partition_keys();
    if (col_idx_in_file >= reader_->getType().getSubtypeCount()) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a nullptr in this slot.
      Tuple** template_tuple = &template_tuple_map_[tuple_desc];
      if (*template_tuple == nullptr) {
        *template_tuple =
            Tuple::Create(tuple_desc->byte_size(), template_tuple_pool_.get());
      }
      (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    selected_indices.push_back(col_idx_in_file);
    col_id_slot_map_[reader_->getType().getSubtype(col_idx_in_file)->getColumnId()] = slot_desc;
    const ColumnType &col_type = scan_node_->hdfs_table()->col_descs()[col_idx].type();
    if (col_type.type == TYPE_ARRAY) {
      // TODO
    } else if (col_type.type == TYPE_MAP) {
      // TODO
    } else if (col_type.type == TYPE_STRUCT) {
      // TODO
    } else {
      DCHECK(!col_type.IsComplexType());
      DCHECK_EQ(path.size(), 1);
      // TODO
      ++num_columns;
    }
  }
  COUNTER_SET(num_cols_counter_, static_cast<int64_t>(num_columns));
  row_reader_options.include(selected_indices);
}

Status HdfsOrcScanner::ProcessSplit() {
  DCHECK(scan_node_->HasRowBatchQueue());
  HdfsScanNode* scan_node = static_cast<HdfsScanNode*>(scan_node_);
  do {
    unique_ptr<RowBatch> batch = std::make_unique<RowBatch>(scan_node_->row_desc(),
        state_->batch_size(), scan_node_->mem_tracker());
    Status status = GetNextInternal(batch.get());
    // Always add batch to the queue because it may contain data referenced by previously
    // appended batches.
    scan_node->AddMaterializedRowBatch(move(batch));
    RETURN_IF_ERROR(status);
    ++row_batches_produced_;
    if ((row_batches_produced_ & (BATCHES_PER_FILTER_SELECTIVITY_CHECK - 1)) == 0) {
      CheckFiltersEffectiveness();
    }
  } while (!eos_ && !scan_node_->ReachedLimit());
  return Status::OK();
}

Status HdfsOrcScanner::GetNextInternal(RowBatch* row_batch) {
  if (scan_node_->IsZeroSlotTableScan()) {
    uint64_t file_rows = reader_->getNumberOfRows();
    // There are no materialized slots, e.g. count(*) over the table.  We can serve
    // this query from just the file metadata.  We don't need to read the column data.
    if (stripe_rows_read_ == file_rows) {
      eos_ = true;
      return Status::OK();
    }
    assemble_rows_timer_.Start();
    DCHECK_LT(stripe_rows_read_, file_rows);
    int64_t rows_remaining = file_rows - stripe_rows_read_;
    int max_tuples = min<int64_t>(row_batch->capacity(), rows_remaining);
    TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
    int num_to_commit = WriteTemplateTuples(current_row, max_tuples);
    Status status = CommitRows(row_batch, num_to_commit);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(status);
    stripe_rows_read_ += max_tuples;
    COUNTER_ADD(scan_node_->rows_read_counter(), num_to_commit);
    return Status::OK();
  }

  // reset tuple memory. We'll allocate it the first time we use it.
  tuple_ = nullptr;

  // Transfer remaining tuples from the scratch batch.
  if (ScratchBatchNotEmpty()) {
    assemble_rows_timer_.Start();
    int num_row_to_commit = TransferScratchTuples(row_batch);
    assemble_rows_timer_.Stop();
    RETURN_IF_ERROR(CommitRows(row_batch, num_row_to_commit));
    if (row_batch->AtCapacity()) return Status::OK();
    DCHECK_EQ(scratch_batch_tuple_idx_, scratch_batch_->numElements);
  }

  while (advance_stripe_ || end_of_stripe_) {
    context_->ReleaseCompletedResources(/* done */ true);
    // Commit the rows to flush the row batch from the previous stripe
    RETURN_IF_ERROR(CommitRows(row_batch, 0));

    RETURN_IF_ERROR(NextStripe());
    DCHECK_LE(stripe_idx_, reader_->getNumberOfStripes());
    if (stripe_idx_ == reader_->getNumberOfStripes()) {
      eos_ = true;
      DCHECK(parse_status_.ok());
      return Status::OK();
    }
  }

  // Apply any runtime filters to static tuples containing the partition keys for this
  // partition. If any filter fails, we return immediately and stop processing this
  // scan range.
  if (!scan_node_->PartitionPassesFilters(context_->partition_descriptor()->id(),
      FilterStats::ROW_GROUPS_KEY, context_->filter_ctxs())) {
    eos_ = true;
    DCHECK(parse_status_.ok());
    return Status::OK();
  }
  assemble_rows_timer_.Start();
  Status status = AssembleRows(row_batch);
  assemble_rows_timer_.Stop();
  RETURN_IF_ERROR(status);
  if (!parse_status_.ok()) {
    RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    parse_status_ = Status::OK();
  }

  return Status::OK();
}

void HdfsOrcScanner::CheckFiltersEffectiveness() {
  for (int i = 0; i < filter_stats_.size(); ++i) {
    LocalFilterStats* stats = &filter_stats_[i];
    const RuntimeFilter* filter = filter_ctxs_[i]->filter;
    double reject_ratio = stats->rejected / static_cast<double>(stats->considered);
    if (filter->AlwaysTrue() ||
        reject_ratio < FLAGS_orc_min_filter_reject_ratio) {
      stats->enabled = 0;
    }
  }
}

inline bool HdfsOrcScanner::ScratchBatchNotEmpty() {
  return scratch_batch_ != nullptr
      && scratch_batch_tuple_idx_ < scratch_batch_->numElements;
}

inline static bool CheckStripeOverlapsSplit(int64_t stripe_start, int64_t stripe_end,
    int64_t split_start, int64_t split_end) {
  return (split_start >= stripe_start && split_start < stripe_end) ||
      (split_end > stripe_start && split_end <= stripe_end) ||
      (split_start <= stripe_start && split_end >= stripe_end);
}

Status HdfsOrcScanner::NextStripe() {
  const ScanRange* split_range = static_cast<ScanRangeMetadata*>(
      metadata_range_->meta_data())->original_split;
  int64_t split_offset = split_range->offset();
  int64_t split_length = split_range->len();

  bool start_with_first_stripe = stripe_idx_ == -1;
  bool misaligned_stripe_skipped = false;

  advance_stripe_ = false;
  stripe_rows_read_ = 0;

  // Loop until we have found a non-empty stripe.
  while (true) {
    // Reset the parse status for the next stripe.
    parse_status_ = Status::OK();

    ++stripe_idx_;
    if (stripe_idx_ >= reader_->getNumberOfStripes()) {
      if (start_with_first_stripe && misaligned_stripe_skipped) {
        // We started with the first stripe and skipped all the stripes because they were
        // misaligned. The execution flow won't reach this point if there is at least one
        // non-empty stripe which this scanner can process.
        COUNTER_ADD(num_scanners_with_no_reads_counter_, 1);
      }
      break;
    }
    unique_ptr<orc::StripeInformation> stripe = reader_->getStripe(stripe_idx_);
    // Also check 'footer_.numberOfRows' to make sure 'select count(*)' and 'select *'
    // behave consistently for corrupt files that have 'footer_.numberOfRows == 0'
    // but some data in stripe.
    if (stripe->getNumberOfRows() == 0 || reader_->getNumberOfRows() == 0) continue;

    // TODO ValidateColumnOffsets

    google::uint64 stripe_offset = stripe->getOffset();
    google::uint64 stripe_len = stripe->getIndexLength() + stripe->getDataLength() +
        stripe->getFooterLength();
    int64_t stripe_mid_pos = stripe_offset + stripe_len / 2;
    if (!(stripe_mid_pos >= split_offset &&
        stripe_mid_pos < split_offset + split_length)) {
      // Middle pos not in split, this stripe will be handled by a different scanner.
      // Mark if the stripe overlaps with the split.
      misaligned_stripe_skipped |= CheckStripeOverlapsSplit(stripe_offset,
          stripe_offset + stripe_len, split_offset, split_offset + split_length);
      continue;
    }

    // TODO: check if this stripe can be skipped by stats.

    COUNTER_ADD(num_stripes_counter_, 1);
    row_reader_options.range(stripe->getOffset(), stripe_len);
    row_reader_ = reader_->createRowReader(row_reader_options);
    end_of_stripe_ = false;
    break;
  }

  DCHECK(parse_status_.ok());
  return Status::OK();
}

Status HdfsOrcScanner::ProcessFileTail() {
  unique_ptr<orc::InputStream> input_stream(new ScanRangeInputStream(this));
  try {
    reader_ = orc::createReader(move(input_stream), reader_options_);
  } catch (std::exception& e) {
    VLOG_QUERY << "Encounter parse error: " << e.what();
    parse_status_ = Status(Substitute("Encounter parse error: $0.", e.what()));
    return parse_status_;
  }

  if (reader_->getNumberOfRows() == 0) {
    // Empty file
    return Status::OK();
  }

  if (reader_->getNumberOfStripes() == 0) {
    return Status(Substitute("Invalid file: $0. No stripes in this file but numberOfRows"
        " in footer is $1", filename(), reader_->getNumberOfRows()));
  }

  return Status::OK();
}

inline THdfsCompression::type HdfsOrcScanner::TranslateCompressionKind(
    orc::CompressionKind kind) {
  switch (kind) {
    case orc::CompressionKind::CompressionKind_NONE:return THdfsCompression::NONE;
    // zlib used in ORC is corresponding to Deflate used in Impala
    case orc::CompressionKind::CompressionKind_ZLIB: return THdfsCompression::DEFLATE;
    case orc::CompressionKind::CompressionKind_SNAPPY: return THdfsCompression::SNAPPY;
    case orc::CompressionKind::CompressionKind_LZO: return THdfsCompression::LZO;
    case orc::CompressionKind::CompressionKind_LZ4: return THdfsCompression::LZ4;
    case orc::CompressionKind::CompressionKind_ZSTD: return THdfsCompression::ZSTD;
  }
  return THdfsCompression::DEFAULT;
}

Status HdfsOrcScanner::AssembleRows(RowBatch* row_batch) {
  bool continue_execution = !scan_node_->ReachedLimit() && !context_->cancelled();
  if (!continue_execution)  return Status::CANCELLED;

  scratch_batch_tuple_idx_ = 0;
  scratch_batch_ = move(row_reader_->createRowBatch(row_batch->capacity()));
  DCHECK_EQ(scratch_batch_->numElements, 0);

  int64_t num_rows_read = 0;
  while (continue_execution) {  // one ORC scratch batch (ColumnVectorBatch) in a round
    if (scratch_batch_tuple_idx_ == scratch_batch_->numElements) {
      try {
        if (!row_reader_->next(*scratch_batch_)) {
          end_of_stripe_ = true;
          break; // no more data to process
        }
      } catch (std::exception& e) {
        if (context_->cancelled()) {
          VLOG_QUERY << "Encounter parse error: " << e.what()
                     << " which is due to cancellation";
          parse_status_ = Status::CANCELLED;
        } else {
          VLOG_QUERY << "Encounter parse error: " << e.what();
          parse_status_ = Status(Substitute("Encounter parse error: $0.", e.what()));
        }
        eos_ = true;
        return parse_status_;
      }
      if (scratch_batch_->numElements == 0) {
        RETURN_IF_ERROR(CommitRows(row_batch, 0));
        end_of_stripe_ = true;
        return Status::OK();
      }
      num_rows_read += scratch_batch_->numElements;
      scratch_batch_tuple_idx_ = 0;
    }

    int num_to_commit = TransferScratchTuples(row_batch);
    RETURN_IF_ERROR(CommitRows(row_batch, num_to_commit));
    if (row_batch->AtCapacity()) break;
    continue_execution &= !scan_node_->ReachedLimit() && !context_->cancelled();
  }
  stripe_rows_read_ += num_rows_read;
  COUNTER_ADD(scan_node_->rows_read_counter(), num_rows_read);
  return Status::OK();
}

int HdfsOrcScanner::TransferScratchTuples(RowBatch* dst_batch) {
  const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();

  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_->data();
  int num_conjuncts = conjunct_evals_->size();

  const orc::Type* root_type = &row_reader_->getSelectedType();
  DCHECK_EQ(root_type->getKind(), orc::TypeKind::STRUCT);

  DCHECK_LT(dst_batch->num_rows(), dst_batch->capacity());
  int row_id = dst_batch->num_rows();
  int capacity = dst_batch->capacity();
  int num_to_commit = 0;
  TupleRow* row = dst_batch->GetRow(row_id);

  if (tuple_ == nullptr) AllocateTupleMem(dst_batch);
  Tuple* tuple = tuple_;  // tuple_ is updated in CommitRows

  while (row_id < capacity && ScratchBatchNotEmpty()) {
    InitTuple(tuple_desc, template_tuple_, tuple);
    ReadRow(*scratch_batch_, scratch_batch_tuple_idx_++, root_type, tuple, dst_batch);
    row->SetTuple(scan_node_->tuple_idx(), tuple);
    if (!EvalRuntimeFilters(row)) continue;
    if (ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts, row)) {
      row = next_row(row);
      tuple = next_tuple(tuple_desc->byte_size(), tuple);
      ++row_id;
      ++num_to_commit;
    }
  }
  return num_to_commit;
}

Status HdfsOrcScanner::AllocateTupleMem(RowBatch* row_batch) {
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);
  DCHECK_GT(row_batch->capacity(), 0);
  return Status::OK();
}

Status HdfsOrcScanner::CommitRows(RowBatch* dst_batch, int num_rows) {
  DCHECK(dst_batch != nullptr);
  dst_batch->CommitRows(num_rows);
  tuple_mem_ += static_cast<int64_t>(scan_node_->tuple_desc()->byte_size()) * num_rows;
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);

  if (context_->cancelled()) return Status::CANCELLED;
  // Check for UDF errors.
  RETURN_IF_ERROR(state_->GetQueryStatus());
  // Clear expr result allocations for this thread to avoid accumulating too much
  // memory from evaluating the scanner conjuncts.
  context_->expr_results_pool()->Clear();
  return Status::OK();
}

bool HdfsOrcScanner::EvalRuntimeFilters(TupleRow* row) {
  int num_filters = filter_ctxs_.size();
  for (int i = 0; i < num_filters; ++i) {
    if (!EvalRuntimeFilter(i, row)) return false;
  }
  return true;
}

bool HdfsOrcScanner::EvalRuntimeFilter(int i, TupleRow* row) {
  LocalFilterStats* stats = &filter_stats_[i];
  const FilterContext* ctx = filter_ctxs_[i];
  ++stats->total_possible;
  if (stats->enabled && ctx->filter->HasFilter()) {
    ++stats->considered;
    if (!ctx->Eval(row)) {
      ++stats->rejected;
      return false;
    }
  }
  return true;
}

inline void HdfsOrcScanner::ReadRow(const orc::ColumnVectorBatch &batch, int row_idx,
    const orc::Type* orc_type, Tuple* tuple, RowBatch* dst_batch) {
  const orc::StructVectorBatch &struct_batch =
      dynamic_cast<const orc::StructVectorBatch &>(batch);
  for (unsigned int c = 0; c < orc_type->getSubtypeCount(); ++c) {
    orc::ColumnVectorBatch* col_batch = struct_batch.fields[c];
    const orc::Type* col_type = orc_type->getSubtype(c);
    const SlotDescriptor* slot_desc = col_id_slot_map_[col_type->getColumnId()];
    if (slot_desc == nullptr) {
      VLOG_QUERY << "slot not found for column " << c << ": " << col_type->toString();
      continue;
    }
    if (col_batch->hasNulls && !col_batch->notNull[row_idx]) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    void* slot_val_ptr = tuple->GetSlot(slot_desc->tuple_offset());
    switch (col_type->getKind()) {
      case orc::TypeKind::BOOLEAN: {
        int64_t val = dynamic_cast<const orc::LongVectorBatch*>(col_batch)->
            data.data()[row_idx];
        *(reinterpret_cast<bool*>(slot_val_ptr)) = (val != 0);
        break;
      }
      case orc::TypeKind::BYTE:
      case orc::TypeKind::SHORT:
      case orc::TypeKind::INT:
      case orc::TypeKind::LONG: {
        const orc::LongVectorBatch* long_batch =
            dynamic_cast<const orc::LongVectorBatch*>(col_batch);
        int64_t val = long_batch->data.data()[row_idx];
        switch (slot_desc->type().type) {
          case TYPE_TINYINT:*(reinterpret_cast<int8_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_SMALLINT:*(reinterpret_cast<int16_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_INT:*(reinterpret_cast<int32_t*>(slot_val_ptr)) = val;
            break;
          case TYPE_BIGINT:*(reinterpret_cast<int64_t*>(slot_val_ptr)) = val;
            break;
          default:
            DCHECK(false) << "Illegal translation from impala type "
                << slot_desc->DebugString() << " to orc INT";
        }
        break;
      }
      case orc::TypeKind::FLOAT:
      case orc::TypeKind::DOUBLE: {
        double val =
            dynamic_cast<const orc::DoubleVectorBatch*>(col_batch)->data.data()[row_idx];
        switch (slot_desc->type().type) {
          case TYPE_FLOAT:*(reinterpret_cast<float*>(slot_val_ptr)) = val;
            break;
          case TYPE_DOUBLE:*(reinterpret_cast<double*>(slot_val_ptr)) = val;
            break;
          default:
            DCHECK(false) << "Illegal translation from impala type "
                << slot_desc->DebugString() << " to orc DOUBLE";
        }
        break;
      }
      case orc::TypeKind::STRING:
      case orc::TypeKind::VARCHAR:
      case orc::TypeKind::CHAR: {
        char* const* start =
            dynamic_cast<const orc::StringVectorBatch*>(col_batch)->data.data();
        const int64_t* length =
            dynamic_cast<const orc::StringVectorBatch*>(col_batch)->length.data();
        StringValue* val_ptr = reinterpret_cast<StringValue*>(slot_val_ptr);
        val_ptr->len = length[row_idx];
        // Space in the StringVectorBatch is allocated by reader_mem_pool_. It will be
        // reused at next batch, so we allocate a new space for this string.
        val_ptr->ptr = reinterpret_cast<char*>(
            dst_batch->tuple_data_pool()->Allocate(val_ptr->len));
        memcpy(val_ptr->ptr, start[row_idx], val_ptr->len);
        break;
      }
      case orc::TypeKind::TIMESTAMP: {
        const orc::TimestampVectorBatch* ts_batch =
            dynamic_cast<const orc::TimestampVectorBatch*>(col_batch);
        int64_t secs = ts_batch->data.data()[row_idx];
        int64_t nanos = ts_batch->nanoseconds.data()[row_idx];
        *reinterpret_cast<TimestampValue*>(slot_val_ptr) =
            TimestampValue::FromUnixTimeNanos(secs, nanos);
        break;
      }
      case orc::TypeKind::DECIMAL: {
        // For decimals whose precision is larger than 18, its value can't fit into
        // an int64 (10^19 > 2^63). So we should use int128 for this case.
        if (col_type->getPrecision() == 0 || col_type->getPrecision() > 18) {
          const orc::Decimal128VectorBatch* int128_batch = dynamic_cast<
              const orc::Decimal128VectorBatch*>(col_batch);
          orc::Int128 orc_val = int128_batch->values.data()[row_idx];

          // TODO warn if slot_desc->type().GetByteSize() != 16
          switch (slot_desc->type().GetByteSize()) {
            case 4:
              reinterpret_cast<Decimal4Value*>(slot_val_ptr)->value() =
                  orc_val.getLowBits();
              break;
            case 8:
              reinterpret_cast<Decimal8Value*>(slot_val_ptr)->value() =
                  orc_val.getLowBits();
              break;
            case 16:
              int128_t val = orc_val.getHighBits();
              val <<= 64;
              val |= orc_val.getLowBits();
              // Use memcpy to avoid gcc generating unaligned instructions like movaps
              // for int128_t. They will raise SegmentFault when addresses are not
              // aligned to 16 bytes.
              memcpy(slot_val_ptr, &val, sizeof(int128_t));
              break;
          }
        } else {
          const orc::Decimal64VectorBatch* int64_batch = dynamic_cast<
              const orc::Decimal64VectorBatch*>(col_batch);
          int64_t val = int64_batch->values.data()[row_idx];

          // TODO warn if slot_desc->type().GetByteSize() == 4
          switch (slot_desc->type().GetByteSize()) {
            case 4:
              reinterpret_cast<Decimal4Value*>(slot_val_ptr)->value() = val;
              break;
            case 8:
              reinterpret_cast<Decimal8Value*>(slot_val_ptr)->value() = val;
              break;
            case 16:
              reinterpret_cast<Decimal16Value*>(slot_val_ptr)->value() = val;
              break;
          }
        }
        break;
      }
      case orc::TypeKind::LIST: break;
      case orc::TypeKind::MAP: break;
      case orc::TypeKind::STRUCT: break;
      case orc::TypeKind::UNION: break;
      default:
        DCHECK(false) << slot_desc->type().DebugString();
    }
  }
}

void HdfsOrcScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  if (row_batch != nullptr) {
    context_->ReleaseCompletedResources(true);
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
    context_->ReleaseCompletedResources(true);
  }
  scratch_batch_.reset(NULL);

  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(template_tuple_pool_->total_allocated_bytes(), 0);

  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  THdfsCompression::type compression_type = THdfsCompression::NONE;
  if (reader_ != nullptr &&
      reader_->getCompression() != orc::CompressionKind::CompressionKind_NONE) {
    compression_type = TranslateCompressionKind(reader_->getCompression());
  }
  scan_node_->RangeComplete(THdfsFileFormat::ORC, compression_type);

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    const FilterStats* stats = filter_ctxs_[i]->stats;
    const LocalFilterStats& local = filter_stats_[i];
    stats->IncrCounters(FilterStats::ROWS_KEY, local.total_possible,
        local.considered, local.rejected);
  }

  CloseInternal();
}

}
