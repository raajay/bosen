#include <petuum_ps/server/server_thread.hpp>
#include <petuum_ps_common/thread/msg_base.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/util/stats.hpp>
#include <petuum_ps_common/thread/mem_transfer.hpp>
#include <cstring>

namespace petuum {

  bool ServerThread::WaitMsgBusy(int32_t *sender_id,
                                 zmq::message_t *zmq_msg,
                                 long timeout_milli __attribute__ ((unused)) ) {
    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))
      (sender_id, zmq_msg);
    while (!received) {
      received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))
        (sender_id, zmq_msg);
    }
    return true;
  }



  bool ServerThread::WaitMsgSleep(int32_t *sender_id,
                                  zmq::message_t *zmq_msg,
                                  long timeout_milli __attribute__ ((unused)) ) {
    (GlobalContext::comm_bus->*(
                                GlobalContext::comm_bus->RecvAny_))(sender_id, zmq_msg);

    return true;
  }



  bool ServerThread::WaitMsgTimeOut(int32_t *sender_id,
                                    zmq::message_t *zmq_msg,
                                    long timeout_milli) {

    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_))
      (sender_id, zmq_msg, timeout_milli);
    return received;
  }



  void ServerThread::InitWhenStart() {
    SetWaitMsg();
  }



  void ServerThread::SetWaitMsg() {
    if (GlobalContext::get_aggressive_cpu()) {
      WaitMsg_ = WaitMsgBusy;
    } else {
      WaitMsg_ = WaitMsgSleep;
    }
  }



  void ServerThread::SetUpCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if (GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_server_info(my_id_);
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }

    comm_bus_->ThreadRegister(comm_config);
  }



  void ServerThread::ConnectToNameNode() {
    int32_t name_node_id = GlobalContext::get_name_node_id();

    ServerConnectMsg server_connect_msg;
    void *msg = server_connect_msg.get_mem();
    int32_t msg_size = server_connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(name_node_id)) {
      comm_bus_->ConnectTo(name_node_id, msg, msg_size);
    } else {
      HostInfo name_node_info = GlobalContext::get_name_node_info();
      std::string name_node_addr = name_node_info.ip + ":" + name_node_info.port;
      comm_bus_->ConnectTo(name_node_id, name_node_addr, msg, msg_size);
    }
    VLOG(5) << "Send connection to Name Node";
  }

  void ServerThread::ConnectToScheduler() {
    int32_t scheduler_id = GlobalContext::get_scheduler_id();

    ServerConnectMsg server_connect_msg;
    void *msg = server_connect_msg.get_mem();
    int32_t msg_size = server_connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(scheduler_id)) {
      comm_bus_->ConnectTo(scheduler_id, msg, msg_size);
    } else {
      HostInfo scheduler_info = GlobalContext::get_scheduler_info();
      std::string scheduler_addr = scheduler_info.ip + ":" + scheduler_info.port;
      comm_bus_->ConnectTo(scheduler_id, scheduler_addr, msg, msg_size);
    }
    VLOG(5) << "Sent connection to Scheduler node";
  }


  void ServerThread::ConnectToReplica() {
    int32_t replica_id = GlobalContext::get_replica_for_server(my_id_);

    ServerConnectMsg server_connect_msg;
    void *msg = server_connect_msg.get_mem();
    int32_t msg_size = server_connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(replica_id)) {
      comm_bus_->ConnectTo(replica_id, msg, msg_size);
    } else {
      HostInfo replica_info = GlobalContext::get_replica_info(replica_id);
      std::string replica_addr = replica_info.ip + ":" + replica_info.port;
      comm_bus_->ConnectTo(replica_id, replica_addr, msg, msg_size);
    }
    VLOG(5) << "Sent connection to replica node";
  }


  int32_t ServerThread::GetConnection(bool *is_client, int32_t *client_id) {
    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    if (msg_type == kClientConnect) {
      ClientConnectMsg msg(zmq_msg.data());
      *is_client = true;
      *client_id = msg.get_client_id();
      VLOG(5) << "Receive connection from worker: " << msg.get_client_id();
    } else if (msg_type == kAggregatorConnect) {
      AggregatorConnectMsg msg(zmq_msg.data());
      *is_client = false;
      *client_id = msg.get_client_id();
      VLOG(5) << "Receive connection from aggregator: " << msg.get_client_id();
    } else {
      LOG(FATAL) << "Server received request from non bgworker/aggregator";
      *is_client = false;
    }
    return sender_id;
  }




  void ServerThread::SendToAllBgThreads(MsgBase *msg) {
    for (const auto &bg_worker_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
        (bg_worker_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void ServerThread::SendToAllAggregatorThreads(MsgBase *msg) {
    for (const auto &aggregator_id : aggregator_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_)) (aggregator_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void ServerThread::InitServer() {

    // neither the name node nor scheduler respond.
    ConnectToNameNode();
    ConnectToScheduler();
    ConnectToReplica();

    // wait for new connections
    int32_t num_connections;
    int32_t num_bgs = 0;
    int32_t num_aggs = 0;
    int32_t num_expected_connections = GlobalContext::get_num_worker_clients()
      + GlobalContext::get_num_aggregator_clients();

    VLOG(5) << "Number of expected connections at server thread: " << num_expected_connections;

    for (num_connections = 0; num_connections < num_expected_connections; ++num_connections) {
      int32_t client_id;
      bool is_client;
      int32_t sender_id = GetConnection(&is_client, &client_id);
      if(is_client) {
        bg_worker_ids_[num_bgs] = sender_id;
        num_bgs++;
      } else {
        aggregator_ids_[num_aggs] = sender_id;
        num_aggs++;
      }
    } // end waiting for connections from aggregator and bg worker

    VLOG(5) << "Total connections from bgthreads: " << num_bgs++;
    VLOG(5) << "Total connections from aggregators: " << num_aggs++;

    server_obj_.Init(my_id_, bg_worker_ids_);
    ClientStartMsg client_start_msg;

    VLOG(5) << "Server Thread - send client start to all bg threads";
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&client_start_msg));

    VLOG(5) << "Server Thread - send client start to all aggregators";
    SendToAllAggregatorThreads(reinterpret_cast<MsgBase*>(&client_start_msg));

  } // end function -- init server




  bool ServerThread::HandleShutDownMsg() {
    // When num_shutdown_bgs reaches the total number of clients, the server
    // reply to each bg with a ShutDownReply message
    ++num_shutdown_bgs_;
    if (num_shutdown_bgs_ == GlobalContext::get_num_worker_clients()) {
      ServerShutDownAckMsg shut_down_ack_msg;
      size_t msg_size = shut_down_ack_msg.get_size();
      for (int i = 0; i < GlobalContext::get_num_worker_clients(); ++i) {
        int32_t bg_id = bg_worker_ids_[i];
        size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
          (bg_id, shut_down_ack_msg.get_mem(), msg_size);
        CHECK_EQ(msg_size, sent_size);
      }
      return true;
    }
    return false;
  }



  void ServerThread::HandleCreateTable(int32_t sender_id,
                                       CreateTableMsg &create_table_msg) {
    int32_t table_id = create_table_msg.get_table_id();

    // I'm not name node
    CreateTableReplyMsg create_table_reply_msg;
    create_table_reply_msg.get_table_id() = create_table_msg.get_table_id();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
      (sender_id, create_table_reply_msg.get_mem(), create_table_reply_msg.get_size());
    CHECK_EQ(sent_size, create_table_reply_msg.get_size());

    TableInfo table_info;
    table_info.table_staleness = create_table_msg.get_staleness();
    table_info.row_type = create_table_msg.get_row_type();
    table_info.row_capacity = create_table_msg.get_row_capacity();
    table_info.oplog_dense_serialized
      = create_table_msg.get_oplog_dense_serialized();
    table_info.row_oplog_type
      = create_table_msg.get_row_oplog_type();
    table_info.dense_row_oplog_capacity
      = create_table_msg.get_dense_row_oplog_capacity();
    server_obj_.CreateTable(table_id, table_info);
  }



  void ServerThread::HandleRowRequest(int32_t sender_id,
                                      RowRequestMsg &row_request_msg) {
    int32_t table_id = row_request_msg.get_table_id();
    int32_t row_id = row_request_msg.get_row_id();
    int32_t clock = row_request_msg.get_clock();
    //int32_t server_clock = server_obj_.GetMinClock();

    VLOG(20) <<  "Handling row request sender=" << sender_id
             << " table_id=" << table_id << " row=" << row_id;

    /* -- we do not buffer the requests in asynchronous mode. Always, reply for an request immediately.
    if (server_clock < clock) {
      // not fresh enough, wait
      server_obj_.AddRowRequest(sender_id, table_id, row_id, clock);
      return;
    }
    */

    uint32_t version = server_obj_.GetBgVersion(sender_id);
    int32_t global_model_version = server_obj_.GetAsyncModelVersion();

    ServerRow *server_row = server_obj_.FindCreateRow(table_id, row_id);

    // row subscribe is a null function ...
    RowSubscribe(server_row, GlobalContext::thread_id_to_client_id(sender_id));

    ReplyRowRequest(sender_id,
                    server_row,
                    table_id,
                    row_id,
                    // server_clock,
                    clock, // we for each we return a model view with its own clock information
                    version,
                    global_model_version);

  } // end function -- handle row request



  void ServerThread::ReplyRowRequest(int32_t bg_id,
                                     ServerRow *server_row,
                                     int32_t table_id,
                                     int32_t row_id,
                                     int32_t client_clock, // earlier we used to return server clock
                                     uint32_t version,
                                     int32_t global_model_version) {

    size_t row_size = server_row->SerializedSize();

    ServerRowRequestReplyMsg server_row_request_reply_msg(row_size);
    server_row_request_reply_msg.get_table_id() = table_id;
    server_row_request_reply_msg.get_row_id() = row_id;
    server_row_request_reply_msg.get_clock() = client_clock;
    server_row_request_reply_msg.get_version() = version;
    server_row_request_reply_msg.get_global_model_version() = global_model_version;

    row_size = server_row->Serialize(server_row_request_reply_msg.get_row_data());
    server_row_request_reply_msg.get_row_size() = row_size;
    MemTransfer::TransferMem(comm_bus_, bg_id, &server_row_request_reply_msg);
  }



  void ServerThread::HandleOpLogMsg(int32_t sender_id,
                                    ClientSendOpLogMsg &client_send_oplog_msg) {

    STATS_SERVER_OPLOG_MSG_RECV_INC_ONE();

    bool is_clock = client_send_oplog_msg.get_is_clock(); // if the oplog also says that client has clocked
    int32_t bg_clock = client_send_oplog_msg.get_bg_clock(); // the value of clock at client
    uint32_t version = client_send_oplog_msg.get_version(); // the bg version of the oplog update

    VLOG(5) << "Received client oplog msg from " << sender_id
            << " orig_version=" << version << " orig_sender=" << sender_id;

    STATS_SERVER_ADD_PER_CLOCK_OPLOG_SIZE(client_send_oplog_msg.get_size());

    // send a copy to replica
    ServerSendOpLogMsg replica_msg(client_send_oplog_msg.get_avai_size());
    replica_msg.get_original_sender_id() = sender_id;
    replica_msg.get_original_version() = version;
    replica_msg.get_global_model_version() = server_obj_.GetAsyncModelVersion();
    memcpy(replica_msg.get_data(), client_send_oplog_msg.get_data(), client_send_oplog_msg.get_avai_size());
    MemTransfer::TransferMem(comm_bus_, GlobalContext::get_replica_for_server(my_id_), &replica_msg);


    int32_t observed_delay;
    STATS_SERVER_ACCUM_APPLY_OPLOG_BEGIN();
    server_obj_.ApplyOpLogUpdateVersion(client_send_oplog_msg.get_data(),
                                        client_send_oplog_msg.get_avai_size(),
                                        sender_id,
                                        version,
                                        &observed_delay);
    STATS_SERVER_ACCUM_APPLY_OPLOG_END();
    STATS_MLFABRIC_SERVER_RECORD_DELAY(observed_delay);

    // TODO add delay to the statistics

    bool clock_changed = false;
    if (is_clock) {
      clock_changed = server_obj_.ClockUntil(sender_id, bg_clock);
      if (clock_changed) {
        // update the stats clock
        STATS_SERVER_CLOCK();
        // we remove the piece of code that will look at buffered updates
        // respond if their clock request is not satisfied. In asynchronous
        // mode, we DO NOT buffer row requests.
      } // end if -- clock changed
    } // end if -- is clock

    // always ack op log receipt, saying the version number for a particular
    // update from a client was applied to the model.

    // ack should be send only when replica also acks
    // SendOpLogAckMsg(sender_id, server_obj_.GetBgVersion(sender_id));

    if (clock_changed) {
      // (raajay): the below does nothing when SSP consistency is desired.
      // Used only for SSPPush, which we discontinued.
      ServerPushRow(clock_changed);
    }
  }


  long ServerThread::ServerIdleWork() {
    return 0;
  }


  long ServerThread::ResetServerIdleMilli() {
    return 0;
  }


  /**
   * Is invoked at the end of handle Oplog msg function.
   */
  void ServerThread::SendOpLogAckMsg(int32_t bg_id, uint32_t version) {
    ServerOpLogAckMsg server_oplog_ack_msg;
    server_oplog_ack_msg.get_ack_version() = version;
    size_t msg_size = server_oplog_ack_msg.get_size();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(bg_id, server_oplog_ack_msg.get_mem(), msg_size);
    CHECK_EQ(msg_size, sent_size);
  }


  void *ServerThread::operator() () {

    ThreadContext::RegisterThread(my_id_);

    STATS_REGISTER_THREAD(kServerThread);

    SetUpCommBus();

    pthread_barrier_wait(init_barrier_);

    // waits for bg worker threads from each worker client to connect. One bg
    // thread from each worker client will connect with a server thread. Each bg
    // worker is also notified that it can start.
    InitServer();

    zmq::message_t zmq_msg;
    int32_t sender_id;
    MsgType msg_type;
    void *msg_mem;
    bool destroy_mem = false;
    long timeout_milli = GlobalContext::get_server_idle_milli();


    // like the bg thread, the server thread also goes on an infinite loop.
    // It processes one message at a time; TODO (raajay) shouldn't we have separate queues for
    // control and data messages.

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

      if (msg_type == kMemTransfer) {
        MemTransferMsg mem_transfer_msg(zmq_msg.data());
        msg_mem = mem_transfer_msg.get_mem_ptr();
        msg_type = MsgBase::get_msg_type(msg_mem);
        destroy_mem = true;
      } else {
        msg_mem = zmq_msg.data();
      }

      switch (msg_type) {
      case kClientShutDown:
        {
          bool shutdown = HandleShutDownMsg();
          if (shutdown) {
            comm_bus_->ThreadDeregister();
            STATS_DEREGISTER_THREAD();
            return 0; // only point for terminating the thread.
          }
          break;
        }
      case kCreateTable:
        {
          CreateTableMsg create_table_msg(msg_mem);
          HandleCreateTable(sender_id, create_table_msg);
          break;
        }
      case kRowRequest:
        {
          // here, handle a clients request for new data
          RowRequestMsg row_request_msg(msg_mem);
          HandleRowRequest(sender_id, row_request_msg);
        }
        break;
      case kClientSendOpLog:
        {
          // here, we decide what to do with the oplog (update) that the client
          // sends.
          ClientSendOpLogMsg client_send_oplog_msg(msg_mem);
          //VLOG(15) << "Received an oplog msg from thread:" << sender_id;
          HandleOpLogMsg(sender_id, client_send_oplog_msg);
        }
        break;
      case kReplicaOpLogAck:
        {
          ReplicaOpLogAckMsg replica_ack_msg(msg_mem);
          ServerOpLogAckMsg server_oplog_ack_msg;
          server_oplog_ack_msg.get_ack_version() = replica_ack_msg.get_ack_version();

          size_t msg_size = server_oplog_ack_msg.get_size();
          size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(replica_ack_msg.get_original_sender(),
                                                                 server_oplog_ack_msg.get_mem(), msg_size);
          CHECK_EQ(msg_size, sent_size);
        }
        break;
      default:
        LOG(FATAL) << "Unrecognized message type " << msg_type;
      }

      if (destroy_mem)
        MemTransfer::DestroyTransferredMem(msg_mem);
    }
  }

}
