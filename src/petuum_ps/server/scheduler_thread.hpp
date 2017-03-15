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
    ~SchedulerThread();
    virtual void *operator() ();
    virtual void ShutDown() {
      Join();
    }

  private:

    void InitScheduler();
    void SetupCommBus();

    // communication functions
    bool HandlePreTransmitPing();

    // the id of the scheduler thread
    int32_t my_id_;
    pthread_barrier_t *init_barrier_;
    CommBus *comm_bus_;
  };
}
