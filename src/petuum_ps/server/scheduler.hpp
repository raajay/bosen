#pragma once

#include <petuum_ps/server/scheduler_thread.hpp>
#include <pthread.h>

namespace petuum {
  class Scheduler {
  public:
    static void Init();
    static void ShutDown();
  private:
    static SchedulerThread *scheduler_thread_;
    static pthread_barrier_t scheduler_barrier_;
  }
}
