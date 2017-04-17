#include <petuum_ps/server/aggregator_thread.hpp>
#include <petuum_ps_common/thread/mem_transfer.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps_common/util/stats.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/thread/msg_base.hpp>

namespace petuum {

  bool AggregatorThread::WaitMsgBusy(int32_t *sender_id,
                                     zmq::message_t *zmq_msg,
                                     long timeout_milli __attribute__ ((unused)) ) {
    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_)) (sender_id, zmq_msg);
    while (!received) {
      received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_)) (sender_id, zmq_msg);
    }
    return true;
  }


  bool AggregatorThread::WaitMsgSleep(int32_t *sender_id,
                                      zmq::message_t *zmq_msg,
                                      long timeout_milli __attribute__ ((unused)) ) {
    (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAny_))(sender_id, zmq_msg);
    return true;
  }


  bool AggregatorThread::WaitMsgTimeOut(int32_t *sender_id,
                                        zmq::message_t *zmq_msg,
                                        long timeout_milli) {
    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_)) (sender_id, zmq_msg, timeout_milli);
    return received;
  }


  void AggregatorThread::InitWhenStart() {
    SetWaitMsg();
  }



  void AggregatorThread::SetWaitMsg() {
    if (GlobalContext::get_aggressive_cpu()) {
      WaitMsg_ = WaitMsgBusy;
    } else {
      WaitMsg_ = WaitMsgSleep;
    }
  }


  void AggregatorThread::SetUpCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if (GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_aggregator_info(my_id_);
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }
    comm_bus_->ThreadRegister(comm_config);
  }


  void AggregatorThread::ConnectToNameNode() {
    int32_t name_node_id = GlobalContext::get_name_node_id();

    AggregatorConnectMsg aggregator_connect_msg;
    aggregator_connect_msg.get_client_id() = my_id_;
    void *msg = aggregator_connect_msg.get_mem();
    int32_t msg_size = aggregator_connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(name_node_id)) {
      comm_bus_->ConnectTo(name_node_id, msg, msg_size);
    } else {
      HostInfo name_node_info = GlobalContext::get_name_node_info();
      std::string name_node_addr = name_node_info.ip + ":" + name_node_info.port;
      comm_bus_->ConnectTo(name_node_id, name_node_addr, msg, msg_size);
    }
    VLOG(1) << "Aggregator: " << my_id_ <<  " successfully connected to name node.";
  }


  void AggregatorThread :: ConnectToScheduler() {
    int32_t scheduler_id = GlobalContext::get_scheduler_id();
    AggregatorConnectMsg agg_connect_msg;
    agg_connect_msg.get_client_id() = GlobalContext::get_client_id();
    void *msg = agg_connect_msg.get_mem();
    int32_t msg_size = agg_connect_msg.get_size();

    if(comm_bus_->IsLocalEntity(scheduler_id)) {
      comm_bus_->ConnectTo(scheduler_id, msg, msg_size);
      VLOG(2) << "Init LOCAL handshake from aggregator thread=" << my_id_ << " to scheduler=" << scheduler_id;
    } else {
      HostInfo scheduler_info = GlobalContext::get_scheduler_info();
      std::string scheduler_addr = scheduler_info.ip + ":" + scheduler_info.port;
      comm_bus_->ConnectTo(scheduler_id, scheduler_addr, msg, msg_size);
      VLOG(2) << "Init handshake from aggregator thread=" << my_id_ << " to scheduler=" << scheduler_id << " at " << scheduler_addr;
    }
  }


  int32_t AggregatorThread::GetConnection(bool *is_client, int32_t *client_id) {

    int32_t sender_id;
    zmq::message_t zmq_msg;

    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    if (msg_type == kClientConnect) {
      ClientConnectMsg msg(zmq_msg.data());

      *is_client = true;
      *client_id = msg.get_client_id();

    } else if (msg_type == kClientStart) {

      ClientStartMsg msg(zmq_msg.data());
      *is_client = false;
      *client_id = 0;

    } else {

      LOG(FATAL) << "Server received request from non bgworker/server";
      *is_client = false;

    }

    VLOG(1) << "[Aggregator Thread:" << my_id_ << " ] Received connection from thread:" << sender_id;
    return sender_id;

  } // end function -- get connection


  void AggregatorThread::SendToAllBgThreads(MsgBase *msg) {
    for (const auto &bg_worker_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_)) (bg_worker_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void AggregatorThread::InitAggregator() {

    ConnectToNameNode();
    ConnectToScheduler();
    // these two will not reply

    // connect to all servers (server will respond with Client Start msg)
    // so get connection later should be cognizant of it
    for (const auto &server_id : server_ids_) {
      ConnectToServer(server_id);
    }

    // wait for connection from all bg threads and replied from all server
    int32_t num_expected_conns = GlobalContext::get_num_worker_clients() + GlobalContext::get_num_server_clients();

    int32_t num_bgs = 0;
    int32_t num_servers = 0;
    int32_t num_connections;

    for (num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
      int32_t client_id;
      bool is_client;
      VLOG(1) << "Waiting for " << num_connections + 1 << " connection.";

      int32_t sender_id = GetConnection(&is_client, &client_id);

      if(is_client) {
        bg_worker_ids_[num_bgs++] = sender_id;
      } else {
        num_servers++;
      }

    } // end for -- over expected connections

    // TODO create an aggregator object similar to server object
    // aggregator_obj_.Init(my_id_, bg_worker_ids_);

    ClientStartMsg client_start_msg;
    VLOG(1) << "[Thread:" << my_id_ << " ] Send Client Start to " << num_bgs << " bg threads.";
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&client_start_msg));
  }


  void AggregatorThread :: ConnectToServer(int32_t server_id) {
    AggregatorConnectMsg aggregator_connect_msg;
    aggregator_connect_msg.get_client_id() = GlobalContext::get_client_id();
    void *msg = aggregator_connect_msg.get_mem();
    int32_t msg_size = aggregator_connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(server_id)) {
      comm_bus_->ConnectTo(server_id, msg, msg_size);
      VLOG(2) << "Init LOCAL handshake from aggregator=" << my_id_ << " to server=" << server_id;
    } else {
      HostInfo server_info;
      server_info = GlobalContext::get_server_info(server_id);
      std::string server_addr = server_info.ip + ":" + server_info.port;
      comm_bus_->ConnectTo(server_id, server_addr, msg, msg_size);
      VLOG(2) << "Init handshake from aggregator=" << my_id_ << " to server="
              << server_id << " at " << server_addr;
    }
  } // end function -- connect to server




  void *AggregatorThread::operator() () {
    ThreadContext::RegisterThread(my_id_);
    STATS_REGISTER_THREAD(kServerThread);
    SetUpCommBus();

    // wait launch all aggregator threads
    pthread_barrier_wait(init_barrier_);

    InitAggregator();

    zmq::message_t zmq_msg;
    int32_t sender_id;
    MsgType msg_type;
    void *msg_mem;
    bool destroy_mem = false;
    long timeout_milli = GlobalContext::get_server_idle_milli();

    while(1) {
      bool received = WaitMsg_(&sender_id, &zmq_msg, timeout_milli);
      if (!received) {
        timeout_milli = ServerIdleWork();
        continue;
      } else {
        timeout_milli = GlobalContext::get_server_idle_milli();
      }

      msg_type = MsgBase::get_msg_type(zmq_msg.data());
      destroy_mem = false;

      if(msg_type == kMemTransfer) {
        MemTransferMsg mem_transfer_msg(zmq_msg.data());
        msg_mem = mem_transfer_msg.get_mem_ptr();
        msg_type = MsgBase::get_msg_type(msg_mem);
        destroy_mem = true;
      } else {
        msg_mem = zmq_msg.data();
      }

      switch(msg_type) {
      default:
        LOG(FATAL) << "Unrecognized message type " << msg_type;
        break;
      }

      if(destroy_mem) {
        MemTransfer::DestroyTransferredMem(msg_mem);
      }

    } // end while -- infinite loop
  }


  long AggregatorThread::ServerIdleWork() {
    return 0;
  }

  long AggregatorThread::ResetServerIdleMilli() {
    return 0;
  }
}
