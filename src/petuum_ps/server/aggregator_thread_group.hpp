// author: raajay
#pragma once

#include <pthread.h>
#include <petuum_ps/server/aggregator_thread.hpp>

namespace petuum {

  class AggregatorThreadGroup {
  public:
    // do we need tables for the constructor
    AggregatorThreadGroup();
    virtual ~AggregatorThreadGroup();

    void Start();
    void ShutDown();
    void AppThreadRegister();
    void AppThreadDeregister();


  protected:

    /* Helper Functions */
    std::vector<AggregatorThread*> aggregator_vec_;
    int32_t aggregator_id_st_;
    pthread_barrier_t init_barrier_;

  private:
    virtual void CreateAggregators();

  };

}
