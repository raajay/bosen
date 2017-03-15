// scheduler_thread.hpp
// author: raajay

#pragma once

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
}
