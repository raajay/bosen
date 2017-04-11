#include <petuum_ps/thread/ssp_bg_worker.hpp>

#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/util/class_register.hpp>
#include <petuum_ps/client/oplog_serializer.hpp>
#include <petuum_ps/client/ssp_client_row.hpp>
#include <petuum_ps_common/util/stats.hpp>
#include <petuum_ps_common/comm_bus/comm_bus.hpp>
#include <petuum_ps_common/thread/mem_transfer.hpp>
#include <petuum_ps/thread/context.hpp>
#include <glog/logging.h>
#include <utility>
#include <limits.h>
#include <algorithm>

namespace petuum {


  SSPBgWorker::SSPBgWorker(int32_t id,
                           int32_t comm_channel_idx,
                           std::map<int32_t, ClientTable* > *tables,
                           pthread_barrier_t *init_barrier,
                           pthread_barrier_t *create_table_barrier):
    AbstractBgWorker(id, comm_channel_idx,
                     tables, init_barrier,
                     create_table_barrier) { }



  SSPBgWorker::~SSPBgWorker() {
    delete row_request_oplog_mgr_;
  }



  void SSPBgWorker::CreateRowRequestOpLogMgr() {
    row_request_oplog_mgr_ = new SSPRowRequestOpLogMgr;
    VLOG(5) << "Create an OpLogMgr in BgWorker with (id=" << my_id_
            << ", comm_channel_idx=" << my_comm_channel_idx_ << ")";
  }



  bool SSPBgWorker::GetRowOpLog(AbstractOpLog &table_oplog,
                                int32_t row_id,
                                AbstractRowOpLog **row_oplog_ptr) {
    return table_oplog.GetEraseOpLog(row_id, row_oplog_ptr);
  }



  void SSPBgWorker::PrepareBeforeInfiniteLoop() { }


  void SSPBgWorker::FinalizeTableStats() { }


  long SSPBgWorker::ResetBgIdleMilli() {
    return 0;
  }

  long SSPBgWorker::BgIdleWork() {
    return 0;
  }


  ClientRow *SSPBgWorker::CreateClientRow(int32_t clock, int32_t global_version, AbstractRow *row_data) {
    return reinterpret_cast<ClientRow*>(new SSPClientRow(clock, global_version, row_data, true));
  }


  BgOpLog *SSPBgWorker::PrepareOpLogsToSend() {
    BgOpLog *bg_oplog = new BgOpLog;

    // Prepare oplogs prepares each table separately.
    for (const auto &table_pair : (*tables_)) {
      int32_t table_id = table_pair.first;

      if (table_pair.second->get_no_oplog_replay()) {
        // TODO (raajay): I do not understand what no-replay means?
        // code path does not go through this condition

        if (table_pair.second->get_oplog_type() == Sparse ||
            table_pair.second->get_oplog_type() == Dense)
          PrepareOpLogsNormalNoReplay(table_id, table_pair.second);
        else if (table_pair.second->get_oplog_type() == AppendOnly)
          PrepareOpLogsAppendOnlyNoReplay(table_id, table_pair.second);
        else
          LOG(FATAL) << "Unknown oplog type = " << table_pair.second->get_oplog_type();

      } else {

        // why are we creating a oplog partition? This is the most common case.
        // Answer: BgOpLog contains all the oplogs that the current worker is
        // responsible for; it includes oplogs across all tables which are
        // partitioned by the table id.

        // stores op logs responsible for the current workers filtered by the current table id
        BgOpLogPartition *bg_table_oplog = 0;

        if (table_pair.second->get_oplog_type() == Sparse || table_pair.second->get_oplog_type() == Dense) {
          bg_table_oplog = PrepareOpLogsNormal(table_id, table_pair.second);
        }
        else if (table_pair.second->get_oplog_type() == AppendOnly) {
          bg_table_oplog = PrepareOpLogsAppendOnly(table_id, table_pair.second);
        }
        else {
          LOG(FATAL) << "Unknown oplog type = " << table_pair.second->get_oplog_type();
        }

        // we add each tables oplog to the overall oplog
        bg_oplog->Add(table_id, bg_table_oplog);
      }

      FinalizeOpLogMsgStats(table_id,
                            &table_num_bytes_by_server_,
                            &server_table_oplog_size_map_);

    } // end for loop over tables
    return bg_oplog;
  }


  BgOpLogPartition *SSPBgWorker::PrepareOpLogsNormal(int32_t table_id,
                                                     ClientTable *table) {

    AbstractOpLog &table_oplog = table->get_oplog();

    GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize;

    if (table->oplog_dense_serialized()) {
      GetSerializedRowOpLogSize = GetDenseSerializedRowOpLogSize;
    } else {
      GetSerializedRowOpLogSize = GetSparseSerializedRowOpLogSize;
    }

    // Get OpLog index. The index will tell which rows have been modified. So
    // the function below, will query an oplog index -- maintained per table at
    // the process level -- and get all the rows that have been modified. Note
    // that it will find modified rows from among the rows for each table that
    // the current bg_thread is responsible for.

    cuckoohash_map<int32_t, bool> *new_table_oplog_index_ptr = table->GetAndResetOpLogIndex(my_comm_channel_idx_);

    size_t table_update_size = table->get_sample_row()->get_update_size();

    BgOpLogPartition *bg_table_oplog = new BgOpLogPartition(table_id,
                                                            table_update_size,
                                                            my_comm_channel_idx_);

    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }

    // iterate over all rows that are potentially modified
    for (auto oplog_index_iter = new_table_oplog_index_ptr->cbegin(); !oplog_index_iter.is_end(); oplog_index_iter++) {

      int32_t row_id = oplog_index_iter->first;

      AbstractRowOpLog *row_oplog = 0;
      bool found = GetRowOpLog(table_oplog, row_id, &row_oplog);

      // if not found, row_id has not been modified in this table
      if (!found) {
        continue;
      }

      // if row_oplog is null, nothing more to do
      if (found && (row_oplog == 0)) {
        continue;
      }

      // this function
      // 1. updates the bytes per server dict,
      // 2. adds the oplog to bg_table_oplog, indexed by row_id
      CountRowOpLogToSend(row_id,
                          row_oplog,
                          &table_num_bytes_by_server_,
                          bg_table_oplog,
                          GetSerializedRowOpLogSize);

    } // end for -- over all rows that have oplog (i.e., those which are modified; obtained from oplog index)

    delete new_table_oplog_index_ptr; // no one else points to this struct, see earlier GetAndResetOpLogIndex function
    return bg_table_oplog;
  }



  BgOpLogPartition *SSPBgWorker::PrepareOpLogsAppendOnly(int32_t table_id,
                                                         ClientTable *table) {
    VLOG(2) << "In PrepareOpLogsAppendOnly";
    GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize;

    if (table->oplog_dense_serialized()) {
      GetSerializedRowOpLogSize = GetDenseSerializedRowOpLogSize;
    } else {
      GetSerializedRowOpLogSize = GetSparseSerializedRowOpLogSize;
    }

    size_t table_update_size
      = table->get_sample_row()->get_update_size();
    BgOpLogPartition *bg_table_oplog = new BgOpLogPartition(table_id,
                                                            table_update_size,
                                                            my_comm_channel_idx_);

    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }

    auto buff_iter = append_only_row_oplog_buffer_map_.find(table_id);
    if (buff_iter != append_only_row_oplog_buffer_map_.end()) {
      AppendOnlyRowOpLogBuffer *append_only_row_oplog_buffer = buff_iter->second;
      append_only_row_oplog_buffer->MergeTmpOpLog();

      int32_t row_id;
      AbstractRowOpLog *row_oplog
        = append_only_row_oplog_buffer->InitReadRmOpLog(&row_id);
      while (row_oplog != 0) {
        CountRowOpLogToSend(row_id, row_oplog, &table_num_bytes_by_server_,
                            bg_table_oplog, GetSerializedRowOpLogSize);

        row_oplog = append_only_row_oplog_buffer->NextReadRmOpLog(&row_id);
      }
    }
    return bg_table_oplog;
  }



  void SSPBgWorker::PrepareOpLogsNormalNoReplay(int32_t table_id,
                                                ClientTable *table) {

    AbstractOpLog &table_oplog = table->get_oplog();

    auto serializer_iter = row_oplog_serializer_map_.find(table_id);

    if (serializer_iter == row_oplog_serializer_map_.end()) {
      RowOpLogSerializer *row_oplog_serializer = new RowOpLogSerializer(table->oplog_dense_serialized(), my_comm_channel_idx_);
      row_oplog_serializer_map_.insert(std::make_pair(table_id, row_oplog_serializer));
      serializer_iter = row_oplog_serializer_map_.find(table_id);
    }
    RowOpLogSerializer *row_oplog_serializer = serializer_iter->second;

    // Get OpLog index. The index will tell which rows have been modified. So the
    // function below, will query a static, oplog index and get all the rows that
    // have been modified. Note that it will find modified rows from among the
    // rows for each table that the current bg_thread is reponsible for.
    cuckoohash_map<int32_t, bool> *new_table_oplog_index_ptr = table->GetAndResetOpLogIndex(my_comm_channel_idx_);

    for (auto oplog_index_iter = new_table_oplog_index_ptr->cbegin(); !oplog_index_iter.is_end(); oplog_index_iter++) {

      int32_t row_id = oplog_index_iter->first;

      OpLogAccessor oplog_accessor;
      bool found = table_oplog.FindAndLock(row_id, &oplog_accessor);

      // if not found, row_id has not been modified in this table
      if (!found) {
        continue;
      }

      AbstractRowOpLog *row_oplog = oplog_accessor.get_row_oplog();

      if (found && (row_oplog == 0)) {
        continue;
      }

      row_oplog_serializer->AppendRowOpLog(row_id, row_oplog);

      row_oplog->Reset();
    }

    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }

    row_oplog_serializer->GetServerTableSizeMap(&table_num_bytes_by_server_);
    delete new_table_oplog_index_ptr;

  } // end function -- prepare op logs normal no replay




  void SSPBgWorker::PrepareOpLogsAppendOnlyNoReplay(int32_t table_id,
                                                    ClientTable *table) {

    auto serializer_iter = row_oplog_serializer_map_.find(table_id);
    if (serializer_iter == row_oplog_serializer_map_.end()) {

      RowOpLogSerializer *row_oplog_serializer = new RowOpLogSerializer(table->oplog_dense_serialized(),
                                                                        my_comm_channel_idx_);

      row_oplog_serializer_map_.insert(std::make_pair(table_id, row_oplog_serializer));
      serializer_iter = row_oplog_serializer_map_.find(table_id);
    }

    RowOpLogSerializer *row_oplog_serializer = serializer_iter->second;

    auto buff_iter = append_only_row_oplog_buffer_map_.find(table_id);
    if (buff_iter != append_only_row_oplog_buffer_map_.end()) {
      AppendOnlyRowOpLogBuffer *append_only_row_oplog_buffer = buff_iter->second;

      int32_t row_id;
      AbstractRowOpLog *row_oplog
        = append_only_row_oplog_buffer->InitReadOpLog(&row_id);
      while (row_oplog != 0) {
        row_oplog_serializer->AppendRowOpLog(row_id, row_oplog);
        row_oplog->Reset();

        row_oplog = append_only_row_oplog_buffer->NextReadOpLog(&row_id);
      }
    }
    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }
    row_oplog_serializer->GetServerTableSizeMap(&table_num_bytes_by_server_);
  }

  void SSPBgWorker::TrackBgOpLog(BgOpLog *bg_oplog) {
    bool tracked = row_request_oplog_mgr_->AddOpLog(version_, bg_oplog);
    ++version_;
    VLOG(5) << "Increment version of bgworker:" << my_id_ << " to " << version_;

    // the below function does nothing.
    row_request_oplog_mgr_->InformVersionInc();

    if (!tracked) {
      delete bg_oplog;
    }
  }

  void SSPBgWorker::CheckAndApplyOldOpLogsToRowData(int32_t table_id,
                                                    int32_t row_id,
                                                    uint32_t version,
                                                    AbstractRow *row_data) {
    if (version + 1 < version_) {
      int32_t version_st = version + 1;
      int32_t version_end = version_ - 1;
      ApplyOldOpLogsToRowData(table_id, row_id, version_st, version_end, row_data);
    }
  }

  void SSPBgWorker::ApplyOldOpLogsToRowData(int32_t table_id,
                                            int32_t row_id,
                                            uint32_t version_st,
                                            uint32_t version_end,
                                            AbstractRow *row_data) {
    STATS_BG_ACCUM_SERVER_PUSH_VERSION_DIFF_ADD(version_end - version_st + 1);

    BgOpLog *bg_oplog = row_request_oplog_mgr_->OpLogIterInit(version_st, version_end);
    uint32_t oplog_version = version_st;

    while (bg_oplog != NULL) {
      BgOpLogPartition *bg_oplog_partition = bg_oplog->Get(table_id);
      // OpLogs that are after (exclusively) version should be applied
      const AbstractRowOpLog *row_oplog = bg_oplog_partition->FindOpLog(row_id);
      if (row_oplog != 0) {
        int32_t column_id;
        const void *update;
        update = row_oplog->BeginIterateConst(&column_id);
        while (update != 0) {
          row_data->ApplyIncUnsafe(column_id, update);
          update = row_oplog->NextConst(&column_id);
        }
      }
      bg_oplog = row_request_oplog_mgr_->OpLogIterNext(&oplog_version);
    }
  }

} // end namespace -- petuum
