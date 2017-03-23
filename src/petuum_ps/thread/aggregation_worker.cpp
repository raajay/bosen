#include <petuum_ps/thread/aggregation_worker.hpp>

namespace petuum {
  AggregationWorker :: AggregationWorker(int32_t id, int32_t comm_channel_idx,
                                         pthread_barrier_t *init_barrier) :
    my_id_(id),
    my_comm_channel_idx_(comm_channel_idx),
    init_barrier_(init_barrier),
    comm_bus_(nullptr) {}


  size_t AggregationWorker :: SendMsg(MsgBase *msg) {
    //TODO complete this information
    return 0;
  }

  void AggregationWorker :: RecvMsg(zmq::message_t &zmq_msg) {
    //TODO complete this information
  }

  void AggregationWorker :: ConnectToServer(int32_t server_id) {
    //TODO complete this information
  }

  void AggregationWorker :: ConnectToScheduler() {
    //TODO complete this information
  }

  void AggregationWorker :: ShutDown() {
    //TODO complete this information
  }

  void AggregationWorker :: AppThreadRegister() {
    //TODO complete this information
  }

  void AggregationWorker :: AppThreadDeregister() {
    //TODO complete this information
  }

}
