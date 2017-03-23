// author: raajay

#pragma once

#include <stdint.h>
#include <petuum_ps_common/util/thread.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps_common/comm_bus/comm_bus.hpp>

namespace petuum {
class AggregationWorker : public Thread {
public:
  AggregationWorker(int32_t id, int32_t comm_channel_idx, pthread_barrier_t *init_barrier);
  virtual ~AggregationWorker();

  void ShutDown();
  void AppThreadRegister();
  void AppThreadDeregister();
  virtual void *operator() ();

protected:
  /* Helper Functions */
  size_t SendMsg(MsgBase *msg);
  void RecvMsg(zmq::message_t &zmq_msg);
  void ConnectToServer(int32_t server_id);
  void ConnectToScheduler();

  int32_t my_id_;
  int32_t my_comm_channel_idx_;
  std::vector<int32_t> server_ids_;
  CommBus* const comm_bus_;
  pthread_barrier_t *init_barrier_;
};

}
