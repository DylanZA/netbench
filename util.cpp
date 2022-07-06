#include "util.h"

#include <fcntl.h>
#include <stdio.h>

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

void checkHugePages(int count) {
  static int already_checked = 0;

  count = already_checked = (already_checked + count);

  int file = open("/proc/sys/vm/nr_hugepages", O_RDWR);
  if (file < 0) {
    die("unable to open /proc/sys/vm/nr_hugepages");
  }

  char buff[128];
  buff[sizeof(buff) - 1] = '\0';
  int res = read(file, &buff, sizeof(buff) - 1);
  if (res <= 0) {
    die("unable to read number of pages");
  }

  int val;
  if (sscanf(buff, "%d", &val) != 1) {
    die("unable to parse:", buff);
  }
  vlog("have ", val, " huge pages available");
  if (val < count) {
    int n = snprintf(buff, sizeof(buff), "%d\n", count);
    res = pwrite(file, buff, n, 0);
    if (res != n) {
      log("unable to write ",
          buff,
          " len=",
          n,
          " to /proc/sys/vm/nr_hugepages, "
          "this might not work res=",
          res);
    }
  }
  close(file);
}

std::string hexdump(void const* p, size_t n) {
  std::stringstream ss;
  ss << "[size=" << n << "] ";
  ss << std::setfill('0') << std::setw(2) << std::hex << std::uppercase;
  char const* p2 = (char const*)p;
  for (size_t i = 0; i < n; i++) {
    ss << (int)p2[i];
  }
  return ss.str();
}
