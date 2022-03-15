HEADERS = $(wildcard *.h)
CXX = g++
CXXFLAGS = $(CXXFLAGS_EXTRA)  -g -O2 -std=c++2a -Wall
LDFLAGS = $(LDFLAGS_EXTRA) -g -O2 -std=c++2a -lboost_thread -lpthread -lboost_program_options -luring
SRCS = $(wildcard *.cpp)
OBJECTS = $(patsubst %.cpp, %.o, $(SRCS))
TARGET = tcprecvbench

$(OBJECTS): Makefile

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) $(CXXFLAGS) $(LDFLAGS) -o $@

clean:
	rm -f *.o
	rm -f $(TARGET)

default: $(TARGET)
all: default

.depend: $(SRCS) $(HEADERS)
	rm -f ./.depend
	$(CXX) $(CXXFLAGS) -MM $^ >> ./.depend
depend: .depend

include .depend