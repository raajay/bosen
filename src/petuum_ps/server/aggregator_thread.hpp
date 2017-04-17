// author: raajay

#pragma once

#include <stdint.h>
#include <petuum_ps_common/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/comm_bus/comm_bus.hpp>

namespace petuum {
class AggregatorThread : public Thread {
public:
  AggregatorThread(int32_t id,
                   int32_t comm_channel_idx,
                   pthread_barrier_t *init_barrier) :
    my_id_(id),
    my_comm_channel_idx_(comm_channel_idx),
    comm_bus_(GlobalContext::comm_bus),
    init_barrier_(init_barrier),
    bg_worker_ids_(GlobalContext::get_num_worker_clients()) {

    GlobalContext::GetServerThreadIDs(my_comm_channel_idx_, &(server_ids_));

  }

  virtual ~AggregatorThread() {}

  void ShutDown() {
    Join(); // defined in thread
  }

  void AppThreadRegister();
  void AppThreadDeregister();


protected:
  static bool WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg, long timeout_milli = -1);
  static bool WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg, long timeout_milli  = -1);
  static bool WaitMsgTimeOut(int32_t *sender_id, zmq::message_t *zmq_msg, long timeout_milli);
  CommBus::WaitMsgTimeOutFunc WaitMsg_;

  virtual void InitWhenStart();
  virtual void SetWaitMsg();

  virtual void InitAggregator();


  void SetUpCommBus();
  void ConnectToNameNode();
  int32_t GetConnection(bool *is_client, int32_t *client_id);


  bool HandleShutDownMsg();
  void HandleCreateTable(int32_t sender_id, CreateTableMsg &create_table_msg);
  void HandleOpLogMsg(int32_t sender_id,
                      ClientSendOpLogMsg &client_send_oplog_msg);

  /* Helper Functions */
  size_t SendMsg(MsgBase *msg);
  void RecvMsg(zmq::message_t &zmq_msg);
  void ConnectToServer(int32_t server_id);
  void ConnectToScheduler();

  virtual long ServerIdleWork();
  virtual long ResetServerIdleMilli();
  virtual void SendOpLogAckMsg(int32_t bg_id, uint32_t version);

  virtual void *operator() ();

  int32_t my_id_;
  int32_t my_comm_channel_idx_;
  std::vector<int32_t> server_ids_;
  std::vector<int32_t> bg_worker_ids_;
  CommBus* const comm_bus_;
  pthread_barrier_t *init_barrier_;
};

}
