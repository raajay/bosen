// scheduler_thread.hpp
// author: raajay

#pragma once

#include <pthread.h>

#include <petuum_ps_common/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/comm_bus/comm_bus.hpp>
#include <boost/unordered_map.hpp>

namespace petuum {

  class StoredValue {
  public:
    StoredValue(int32_t bg_id, int32_t unique_id, int32_t server_id) {
      bg_id_ = bg_id;
      unique_id_ = unique_id;
      destination_server_id_ = server_id;
    }
    int32_t bg_id_;
    int32_t unique_id_;
    int32_t destination_server_id_;
  };


  class SchedulerThread : public Thread {
  public:
    SchedulerThread(pthread_barrier_t *init_barrier);
    ~SchedulerThread() {}
    virtual void *operator() ();
    virtual void ShutDown() {
      Join();
    }

  protected:
    virtual void InitWhenStart() {}

  private:

    void InitScheduler();
    void SetupCommBus();

    // communication functions
    int32_t GetConnection(bool *is_client, int32_t *client_id);
    void SendToAllBgThreads(MsgBase* msg);
    size_t SendMsg(int32_t destination_id, MsgBase *msg);


    // communication functions
    bool HandleTransferRequest(int32_t bg_id, TransferRequestMsg &request_msg);
    bool HandleTransferDelivered(int32_t server_id, TransferDeliveredMsg &delivered_msg);

    // internal data structures
    int32_t my_id_;    // the id of the scheduler thread
    pthread_barrier_t *init_barrier_; // a barrier to set up comm_buss
    CommBus *comm_bus_;
    std::vector<int32_t> bg_worker_ids_;



    // these should be indexed by server client id as opposed to server id
    boost::unordered_map<int32_t, std::deque<StoredValue> > storage_;
    boost::unordered_map<int32_t, int32_t> version_counter_;
    boost::unordered_map<int32_t, int32_t> pending_;



    int32_t get_num_queued(int32_t nic_id) {
      if(storage_.find(nic_id) == storage_.end()) {
        return 0;
      } else {
        return storage_[nic_id].size();
      }
    }


    bool is_request_queued(int32_t nic_id) {
      if(storage_.find(nic_id) == storage_.end()) {
        return false;
      } else {
        if(storage_[nic_id].empty()) {
          return false;
        } else {
          return true;
        }
      }
    }


    int32_t get_server_nic_id(int32_t server_client_id) {
      return 1;
    }

  };
}
