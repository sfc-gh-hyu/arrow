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

set(LZO_LIB_NAMES lzo2)

if(LZO_ROOT)
  find_library(LZO_LIB
               NAMES ${LZO_LIB_NAMES}
               PATHS ${LZO_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(LZO_INCLUDE_DIR
            NAMES lzo/lzoconf.h
            PATHS ${LZO_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})

else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(LZO_PC liblzo2)
  if(LZO_PC_FOUND)
    set(LZO_INCLUDE_DIR "${LZO_PC_INCLUDEDIR}")

    list(APPEND LZO_PC_LIBRARY_DIRS "${LZO_PC_LIBDIR}")
    find_library(LZO_LIB
                 NAMES ${LZO_LIB_NAMES}
                 PATHS ${LZO_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  else()
    find_library(LZO_LIB
                 NAMES ${LZ4_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(LZO_INCLUDE_DIR NAMES lzo/lzoconf.h PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(Lzo REQUIRED_VARS LZO_LIB LZO_INCLUDE_DIR)

if(Lzo_FOUND)
  set(Lzo_FOUND TRUE)
  add_library(LZO::lzo UNKNOWN IMPORTED)
  set_target_properties(LZO::lzo
                        PROPERTIES IMPORTED_LOCATION "${LZO_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${LZO_INCLUDE_DIR}")
endif()
