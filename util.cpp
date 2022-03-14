#include "util.h"

float logTime() {
  using namespace std::chrono;
  static auto base = steady_clock::now();
  return float(duration_cast<milliseconds>((steady_clock::now() - base))
                   .count()) /
      1000.0;
}

bool verb = false;
void setVerbose() {
  verb = true;
}
bool isVerbose() {
  return verb;
}
