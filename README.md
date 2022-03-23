# netbench

will put some more stuff here, but some example cmd lines:


run a simple benchmark for io_uring and epoll in one process
 $ ./netbench

prepare an io_uring listener
` $ ./netbench --server_only 1 --rx io_uring`

prepare an epoll listener on a given port
`  $ ./netbench --server_only 1 --rx epoll --use_port 1234`

# run a test to a prepared host
` $ ./netbench --client_only 1 --use_port 11383 --host "foo"`
