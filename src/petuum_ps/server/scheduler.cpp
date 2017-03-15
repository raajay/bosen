#include <petuum_ps/server/scheduler.hpp>

namespace petuum {
    // declare the static variables again in this file
    SchedulerThread *Scheduler::scheduler_thread_;
    pthread_barrier_t Scheduler::init_barrier_;

    void Scheduler::Init() {
        pthread_barrier_init(&init_barrier_, NULL, 2);
        scheduler_thread_ = new SchedulerThread(&init_barrier_);
        scheduler_thread_->Start();
        pthread_barrier_wait(&init_barrier_);
    }

    void Scheduler::ShutDown() {
        scheduler_thread_->ShutDown();
        delete scheduler_thread_;
    }
}
