# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/io")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/io")

set(IO_SRCS
        array_input_stream.cpp
        compressed_input_stream.cpp
        fd_output_stream.cpp
        fd_input_stream.cpp
        io_profiler.cpp
        seekable_input_stream.cpp
        readable.cpp
        s3_input_stream.cpp
        s3_output_stream.cpp
        cache_input_stream.cpp
        cache_select_input_stream.hpp
        shared_buffered_input_stream.cpp
        async_flush_output_stream.cpp
        direct_s3_output_stream.cpp
        )

# Add io_uring sources if enabled
if (WITH_IO_URING)
    list(APPEND IO_SRCS
        io_uring_context.cpp
        io_uring_manager.cpp
        io_uring_input_stream.cpp
        io_uring_buffer_pool.cpp
        )
endif()

add_library(IO STATIC ${IO_SRCS})
