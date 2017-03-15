// scheduler_thread.cpp
// author: raajay

#include <petuum_ps/server/scheduler_thread.hpp>
#include <petuum_ps/thread/context.hpp>
#include <pthread.h>

namespace petuum {

  // constructor - just to initialize the variables
  SchedulerThread::SchedulerThread(pthread_barrier_t *init_barrier):
    my_id_(GlobalContext::get_scheduler_id()), // the id of the scheduler is by default 900
    init_barrier_(init_barrier),
    comm_bus_(GlobalContext::comm_bus) {
  }

  void *SchedulerThread::operator() () {
    ThreadContext::RegisterThread(my_id_);

    SetupCommBus(); // TODO define & implement

    // one this location has been hit, the thread that initialized the scheduler thread can proceed.
    // this ensure, that comm_bus is set up after the thread has been created.
    pthread_barrier_wait(init_barrier_);

    InitScheduler(); // TODO define and implement

    zmq::message_t zmq_msg;
    int32_t sender_id;
    // poll, for new messages
    while(1) {
    }
  }

  void SchedulerThread::SetupCommBus() {
  }

  void SchedulerThread::InitScheduler() {
  }
}
