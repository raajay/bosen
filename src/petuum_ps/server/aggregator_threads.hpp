// author: raajay

#pragma once

#include <petuum_ps/server/aggregator_group.hpp>

namespace petuum {

class Aggregators {
public:
  static void Start();
  static void ShutDown();
  static void AppThreadRegister();
  static void AppThreadDeregister();

private:
  static AggregatorThreadGroup *aggregator_group_;
};

}
