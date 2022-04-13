# netbench

will put some more stuff here, but some example cmd lines:

# Building

## Requirements

netbench requires both boost and liburing to be available.

## Making 

Typically you should run simply to produce ./netbench:

` $ make`

to build an ASAN version (for development) you can run:

` $ make sanitized`

which produces netbench.asan.

To use a custom liburing for example you can run:

` $ make CXXFLAGS_EXTRA="-I<path_to_liburing>/src/include" LDFLAGS_EXTRA="-L <path_to_liburing>/src/ -l:liburing.a"`

To use clang for example you can run
` $ make CXX=clang++`


# Running

## Single process

You can run netbench in single process mode, and it will connect to itself

run a simple benchmark for io_uring and epoll in one process
` $ ./netbench`
 
run for 60 seconds epoll only
` $ ./netbench --rx epoll --time 60`

run for io_uring in two parameterisations
` $ ./netbench --rx "io_uring --provide_buffers 0" --rx "io_uring --provide_buffers 1"`

see options for io_uring engine
` $ ./netbench --rx "io_uring --help"`

## You can also run it on two machines

prepare an io_uring listener on port 10001
` $ ./netbench --server_only 1 --rx io_uring --use_port 10001`

prepare an *IPv4* io_uring and epoll listener starting on port 10001
epoll will then get port 10002
` $ ./netbench --v6 0 --server_only 1 --rx io_uring --rx epoll --use_port 10001`


run tests to a prepared host. the host has 2 ports (eg io_uring and epoll) so use *both*
` $ ./netbench --tx small --tx burst --client_only 1  --host $(dig +short aaaa foo) --use_port 10001 --use_port 10002`

run a test to a IPv4 prepared host
` $ ./netbench --v6 0 --tx small --tx burst --client_only 1 --use_port 10001 --host $(dig +short a foo)`

## Note

By default it uses IPv6, in order to use IPv4 set the v6 flag to 0:
` $ ./netbench --v6 0`
