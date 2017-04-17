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
   * Set up communication bus: Register the thread with comm_bus and use it for all further
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

  } // end function  -- set up comm bus



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
      VLOG(10) << "Scheduler received connection from client: " << msg.get_client_id();

    } else if (msg_type == kAggregatorConnect) {

      AggregatorConnectMsg msg(zmq_msg.data());
      *is_client = false;
      VLOG(10) << "Scheduler received connection from aggregator: " << msg.get_client_id();

    } else if (msg_type == kServerConnect) {

      *is_client = false;
      VLOG(10) << "Scheduler received connection from a server.";

    } else {
      *is_client = false;
      CHECK_EQ(true, false) << "Message other than Client connect msg received on init scheduler.";
    }

    return sender_id;

  } // end function -- get connection



  /*
   * SendToAllBgThreads: A utility function to broadcast message to all background threads.
   */
  void SchedulerThread::SendToAllBgThreads(MsgBase *msg) {
    for(const auto &bg_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(bg_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }


  /*
   * InitScheduler completes the handshake with all the background worker threads.
   */
  void SchedulerThread::InitScheduler() {

    // we expect connections from all bg workers threads on all clients
    int32_t num_expected_conns = GlobalContext::get_num_total_bg_threads() +
      GlobalContext::get_num_total_aggregator_threads() +
      GlobalContext::get_num_total_server_threads();

    int32_t num_bgs = 0; // total number of background worker threads
    int32_t num_servers = 0;

    VLOG(10) << "Number of expected connections at scheduler=" << num_expected_conns;
    int32_t num_connections;
    for(num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
      int32_t client_id;
      bool is_client;
      int32_t sender_id = GetConnection(&is_client, &client_id);
      if(is_client) {
        bg_worker_ids_[num_bgs++] = sender_id;
      } else {
        num_servers++;
      }
    }

    CHECK_EQ(num_bgs, GlobalContext::get_num_total_bg_threads());
    VLOG(5) << "Total connections received:" << num_connections;

    VLOG(5) << "Name node - send connect server to all bg threads";
    ConnectServerMsg connect_server_msg;
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&connect_server_msg));
  }



  bool SchedulerThread::HandlePreTransmitPing() {
    return false;
  }




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

        } // end switch

    } // end while -- infinite loop

  } // end function -- operator




} // end namespace -- petuum
