all: *.cpp *.h
	g++ -g -O2 -o tcprecvbench *.cpp -std=c++2a -lboost_thread -lpthread -lboost_program_options -luring
