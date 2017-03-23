// author: raajay
#pragma once

#include <pthread.h>
#include <petuum_ps/thread/aggregation_worker.hpp>

namespace petuum {

class AggregatorGroup {
public:
  // do we need tables for the constructor
  AggregatorGroup();
  virtual ~AggregatorGroup();

  void Start();
  void ShutDown();
  void AppThreadRegister();
  void AppThreadDeregister();


protected:

  /* Helper Functions */
  std::vector<AggregationWorker*> aggregator_vec_;
  int32_t aggregator_id_st_;

  pthread_barrier_t init_barrier_;
  
private:
  virtual void CreateAggregators();

};

}
