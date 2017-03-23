// author: raajay

#pragma once

#include <petuum_ps/thread/aggregator_group.hpp>

namespace petuum {

class Aggregators {
public:
  static void Start();
  static void ShutDown();
  static void AppThreadRegister();
  static void AppThreadDeregister();

private:
  static AggregatorGroup *aggregator_group_;
};

}