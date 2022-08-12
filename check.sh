#!/use/bin/bash
set -euo pipefail

TARGET=$1

echo "using executable $TARGET"

# don't test fixed_files as older kernels dont have it
# old provide buffers implementations were a bit poor
$TARGET --tx epoll --rx epoll --rx "io_uring --register_ring 0 --provide_buffers 0 --fixed_files 0" --time 1

$TARGET --tx "io_uring  --threads 1 --per_thread 16" --rx "io_uring --register_ring 0 --provide_buffers 1 --fixed_files 0" --time 1
