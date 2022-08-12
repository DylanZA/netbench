HEADERS = $(wildcard *.h)
CXX = g++
CXXFLAGS ?=  -g -O2 -std=c++2a -Wall
LDFLAGS ?= $(LDFLAGS_EXTRA) -g -O2 -std=c++2a -lpthread
SRCS = $(wildcard *.cpp)
OBJECTS = $(patsubst %.cpp, %.o, $(SRCS))
TARGET = netbench
SANITIZED_TARGET = $(TARGET).asan
STATIC_LIBS = 0
SUBMODULE_LIBURING = 0

LDFLAGS += $(LDFLAGS_EXTRA)
CXXFLAGS += $(CXXFLAGS_EXTRA)

ifeq ($(STATIC_LIBS), 1)
LDFLAGS += -l:libboost_thread.a  -l:libboost_system.a -l:libboost_program_options.a -l:liburing.a
else
LDFLAGS += -lboost_thread  -lboost_system -lboost_program_options -luring
endif

default: $(TARGET)
.PHONY: all submodule_liburing

submodule_liburing:

ifeq ($(SUBMODULE_LIBURING), 1)
LDFLAGS += -L liburing/src
CXXFLAGS += -I liburing/src/include

submodule_liburing:
	@$(MAKE) -C liburing
$(OBJECTS): submodule_liburing
endif

$(OBJECTS): Makefile

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) $(CXXFLAGS) $(LDFLAGS) -o $@

clean:
	rm -f *.o
	rm -f $(TARGET)

all: default

.depend: $(SRCS) $(HEADERS)
	rm -f ./.depend
	$(CXX) $(CXXFLAGS) -MM $^ >> ./.depend
depend: .depend

include .depend

# sanitized doesnt use objects because too complex
$(SANITIZED_TARGET): $(SRCS) $(HEADERS)
	$(CXX) $(SRCS)  -fsanitize=address $(CXXFLAGS) $(LDFLAGS) -o $(SANITIZED_TARGET)

sanitized: $(SANITIZED_TARGET)

check: $(SANITIZED_TARGET) $(TARGET)
	bash ./check.sh ./$(SANITIZED_TARGET)
	bash ./check.sh ./$(TARGET)
