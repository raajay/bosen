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

    SetupCommBus();

    // one this location has been hit, the thread that initialized the scheduler thread can proceed.
    // this ensure, that comm_bus is set up after the thread has been created.
    pthread_barrier_wait(init_barrier_);

    InitScheduler(); // TODO define and implement

    zmq::message_t zmq_msg;
    int32_t sender_id;
    // poll, for new messages
    while(1) {
        // recv a packet.
        (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
        MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
        switch(msg_type) {
            case kTransferRequest:
                HandlePreTransmitPing();
                break;

            default:
              LOG(FATAL) << "Unrecognized message type " << msg_type
                  << " sender = " << sender_id;
        }
    }

  }

  void SchedulerThread::SetupCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if(GlobalContext::get_num_client() > 1) {
      comm_config.ltype = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_scheduler_info();
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype = CommBus::kInProc;
    }

    // register the server thread with the commbus. This, basically,
    // creates sockets for this thread, and updates some static variables
    // in comm_bus.
    comm_bus_->ThreadRegister(comm_config);
    std::cout << "The scheduler is up and running!" << std::endl;
  }

  void SchedulerThread::InitScheduler() {
  }

  bool SchedulerThread::HandlePreTransmitPing() {
      return false;
  }
}
