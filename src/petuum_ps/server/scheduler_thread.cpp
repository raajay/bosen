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
    comm_bus_(GlobalContext::comm_bus),
    bg_worker_ids_(GlobalContext::get_num_total_bg_threads()) {}


  /*
   * operator(): The entry point for the main function for all threads.
   */
  void *SchedulerThread::operator() () {
    ThreadContext::RegisterThread(my_id_);

    SetupCommBus();

    // one this location has been hit, the thread that initialized the scheduler
    // thread can proceed. this ensure, that comm_bus is set up after the thread
    // has been created.
    pthread_barrier_wait(init_barrier_);

    // this function waits till all background threads have sent their request
    // to connect. it also responds to each background thread with a 'OK'
    // response.
    InitScheduler();

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



  /*
   * SetupCommBus: Register the thread with comm_bus and use it for all further
   * communications.
   */
  void SchedulerThread::SetupCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if(GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_scheduler_info();
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }

    // register the server thread with the commbus. This, basically,
    // creates sockets for this thread, and updates some static variables
    // in comm_bus.
    comm_bus_->ThreadRegister(comm_config);
    std::cout << "The scheduler is up and running!" << std::endl;
  }



  /*
   * GetConnection: Receive a connection initiating message from background threads.
   */
  int32_t SchedulerThread::GetConnection(bool *is_client, int32_t *client_id) {
    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    if(msg_type == kClientConnect) {
      ClientConnectMsg msg(zmq_msg.data());
      *is_client = true;
      *client_id = msg.get_client_id();
    } else {
      *is_client = false;
      CHECK_EQ(true, false) << "Message other than ClientConnectMsg received on InitScheduler.";
    }
    return sender_id;
  }



  /*
   * SendToAllBgThreads: A utility function to broadcast message to all background threads.
   */
  void SchedulerThread::SendToAllBgThreads(MsgBase *msg) {
    for(const auto &bg_id : bg_worker_ids_) {
      // TODO see if you can replace it with just send (rather than the complex typecasting)
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(bg_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }



  /*
   * InitScheduler completes the handshake with all the background worker threads.
   */
  void SchedulerThread::InitScheduler() {
    // we expect connections from all bg workers threads on all clients
    int32_t num_expected_conns = GlobalContext::get_num_total_bg_threads();
    int32_t num_bgs = 0; // total number of background worker threads

    VLOG(15) << "Number of expected connections at name node=" << num_expected_conns;
    for(int32_t num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
      int32_t client_id;
      bool is_client;
      int32_t sender_id = GetConnection(&is_client, &client_id);
      if(is_client) {
        bg_worker_ids_[num_bgs++] = sender_id;
      }
      VLOG(15) << "Scheduler received connect request from thread:" << sender_id
              << ", #bgs=" << num_bgs;
    }

    CHECK_EQ(num_bgs, GlobalContext::get_num_total_bg_threads());
    VLOG(2) << "Received connect request from " << num_bgs << " bg worker threads.";

    ConnectServerMsg connect_server_msg;
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&connect_server_msg));
  }



  bool SchedulerThread::HandlePreTransmitPing() {
    VLOG(2) << "In HandlePreTransmitPing";
    return false;
  }

} // end namespace -- petuum
