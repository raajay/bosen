// scheduler_thread.hpp
// author: raajay

#pragma once

#include <pthread.h>

#include <petuum_ps_common/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

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

    // communication functions
    bool HandlePreTransmitPing();

    int32_t my_id_;
  };
}
