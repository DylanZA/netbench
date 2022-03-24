#!/use/bin/bash
set -euo pipefail

TARGET=$1

echo "using executable $TARGET"

# don't test fixed_files as older kernels dont have it
# old provide buffers implementations were a bit poor
$TARGET --tx small --tx burst --rx epoll --rx "io_uring --provide_buffers 0 --fixed_files 0" --time 1

$TARGET --tx small  --send_threads 1 --send_connections_per_thread 16 --rx "io_uring --provide_buffers 1 --fixed_files 0" --time 1
