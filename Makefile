HEADERS = $(wildcard *.h)
CXX = g++
CXXFLAGS = $(CXXFLAGS_EXTRA)  -g -O2 -std=c++2a -Wall
LDFLAGS = $(LDFLAGS_EXTRA) -g -O2 -std=c++2a -lpthread
SRCS = $(wildcard *.cpp)
OBJECTS = $(patsubst %.cpp, %.o, $(SRCS))
TARGET = netbench
SANITIZED_TARGET = $(TARGET).asan
STATIC_LIBS = 0

ifeq ($(STATIC_LIBS), 1)
LDFLAGS += -l:libboost_thread.a  -l:libboost_system.a -l:libboost_program_options.a -l:liburing.a
else
LDFLAGS += -lboost_thread  -lboost_system -lboost_program_options -luring
endif

default: $(TARGET)

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
