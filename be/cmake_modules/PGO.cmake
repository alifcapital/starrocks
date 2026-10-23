# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Instrumentation-based PGO for GCC and Clang pre-live builds.
if (NOT PGO_MODE MATCHES "^(OFF|GENERATE|USE)$")
    message(FATAL_ERROR "PGO_MODE must be OFF, GENERATE or USE")
endif()
if (NOT PGO_MODE STREQUAL "OFF")
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$" OR
        NOT CMAKE_C_COMPILER_ID STREQUAL CMAKE_CXX_COMPILER_ID)
        message(FATAL_ERROR "PGO requires matching GCC or Clang C/C++ compilers")
    endif()
    if (NOT CMAKE_BUILD_TYPE STREQUAL "RELEASE" OR WITH_GCOV)
        message(FATAL_ERROR "PGO requires Release without coverage")
    endif()
    if (NOT IS_ABSOLUTE "${PGO_PROFILE_DIR}" OR PGO_PROFILE_DIR MATCHES "[^A-Za-z0-9_./:+-]")
        message(FATAL_ERROR "PGO_PROFILE_DIR must be absolute and use only letters, digits, _, ., /, :, + and -")
    endif()
    if (PGO_MODE STREQUAL "GENERATE")
        # Atomic counters, which GCC picks for -pthread, stop vectorization and make all threads update one cache
        # line: the training BE ran queries up to 40 times slower. Plain counters lose some updates when threads race,
        # which the profile-use build corrects (-fprofile-correction).
        set(_pgo_flags "-fprofile-generate=${PGO_PROFILE_DIR} -fprofile-update=single")
        # The training binary only counts. -g does not change the generated code, so the profile fits the optimized
        # build, and without debug information the training binary links much faster.
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g0")
        set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g0")
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        file(GLOB_RECURSE _pgo_data "${PGO_PROFILE_DIR}/*.gcda")
        if (NOT _pgo_data)
            message(FATAL_ERROR "No .gcda files in PGO_PROFILE_DIR; collect and merge profiles first")
        endif()
        # Unexecuted translation units are expected. Stale/mismatching profiles remain errors.
        # Production runs queries that training did not. GCC optimizes code that training never ran for size, so we
        # keep it optimized for speed with -fprofile-partial-training.
        set(_pgo_flags "-fprofile-use=${PGO_PROFILE_DIR} -fprofile-correction -fprofile-partial-training -Wno-error=missing-profile -Werror=coverage-mismatch")
    else()
        if (NOT EXISTS "${PGO_PROFILE_DIR}/merged.profdata")
            message(FATAL_ERROR "Missing merged.profdata; run llvm-profdata merge first")
        endif()
        set(_pgo_flags "-fprofile-use=${PGO_PROFILE_DIR}/merged.profdata -Werror=profile-instr-out-of-date")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${_pgo_flags}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${_pgo_flags}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${_pgo_flags}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${_pgo_flags}")
endif()
