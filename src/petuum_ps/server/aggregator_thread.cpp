#include <petuum_ps/server/aggregator_thread.hpp>

namespace petuum {
  AggregatorThread :: AggregatorThread(int32_t id, int32_t comm_channel_idx,
                                         pthread_barrier_t *init_barrier) :
    my_id_(id),
    my_comm_channel_idx_(comm_channel_idx),
    comm_bus_(nullptr),
    init_barrier_(init_barrier) {}


  size_t AggregatorThread :: SendMsg(MsgBase *msg) {
    //TODO complete this information
    return 0;
  }

  void AggregatorThread :: RecvMsg(zmq::message_t &zmq_msg) {
    //TODO complete this information
  }

  void AggregatorThread :: ConnectToServer(int32_t server_id) {
    //TODO complete this information
  }

  void AggregatorThread :: ConnectToScheduler() {
    //TODO complete this information
  }

  void AggregatorThread :: ShutDown() {
    //TODO complete this information
  }

  void AggregatorThread :: AppThreadRegister() {
    //TODO complete this information
  }

  void AggregatorThread :: AppThreadDeregister() {
    //TODO complete this information
  }

}
