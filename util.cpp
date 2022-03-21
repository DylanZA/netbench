#include "util.h"

namespace po = boost::program_options;

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

boost::program_options::variables_map simpleParse(
    boost::program_options::options_description desc,
    std::vector<std::string> const& splits) {
  desc.add_options()("help", "produce help message");
  boost::program_options::variables_map vm;
  {
    std::vector<char const*> split_chars;
    for (auto const& s : splits) {
      split_chars.push_back(s.c_str());
    }
    po::store(
        po::parse_command_line(split_chars.size(), split_chars.data(), desc),
        vm);
  }
  po::notify(vm);
  if (vm.count("help")) {
    std::cerr << desc << "\n";
    exit(1);
  }
  return vm;
}
