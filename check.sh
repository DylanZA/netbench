#!/use/bin/bash
set -euo pipefail

TARGET=$1

echo "using executable $TARGET"

# don't test fixed_files as older kernels dont have it
# old provide buffers implementations were a bit poor
$TARGET --v6 0 --tx epoll --rx epoll --rx "io_uring --register_ring 0 --provide_buffers 0 --fixed_files 0" --time 1
