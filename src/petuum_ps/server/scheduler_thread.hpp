// scheduler_thread.hpp
// author: raajay

#pragma once

#include <pthread.h>

#include <petuum_ps_common/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/comm_bus/comm_bus.hpp>

namespace petuum {

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

    // internal data structures
    int32_t my_id_;    // the id of the scheduler thread
    pthread_barrier_t *init_barrier_; // a barrier to set up comm_buss
    CommBus *comm_bus_;
    std::vector<int32_t> bg_worker_ids_;

  };
}
