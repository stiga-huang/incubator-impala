# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/orc")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/orc")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

INCLUDE(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
    #include<fcntl.h>
    #include<unistd.h>
    int main(int,char*[]){
      int f = open(\"/x/y\", O_RDONLY);
      char buf[100];
      return pread(f, buf, 100, 1000) == 0;
    }"
  HAS_PREAD
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    int main(int,char* argv[]){
      return static_cast<int>(std::stoll(argv[0]));
    }"
  HAS_STOLL
)

CHECK_CXX_SOURCE_COMPILES("
    #include<stdint.h>
    #include<stdio.h>
    int main(int,char*[]){
      int64_t x = 1; printf(\"%lld\",x);
    }"
  INT64_IS_LL
)

CHECK_CXX_SOURCE_COMPILES("
    #ifdef __clang__
      #pragma clang diagnostic push
      #pragma clang diagnostic ignored \"-Wdeprecated\"
      #pragma clang diagnostic pop
   #elif defined(__GNUC__)
      #pragma GCC diagnostic push
      #pragma GCC diagnostic ignored \"-Wdeprecated\"
      #pragma GCC diagnostic pop
   #else
     unknownCompiler!
   #endif
   int main(int, char *[]) {}"
  HAS_DIAGNOSTIC_PUSH
)

CHECK_CXX_SOURCE_COMPILES("
    #include<cmath>
    int main(int, char *[]) {
      return std::isnan(1.0f);
    }"
  HAS_STD_ISNAN
)

CHECK_CXX_SOURCE_COMPILES("
    #include<mutex>
    int main(int, char *[]) {
       std::mutex test_mutex;
       std::lock_guard<std::mutex> lock_mutex(test_mutex);
    }"
  HAS_STD_MUTEX
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    std::string func() {
      std::string var = \"test\";
      return std::move(var);
    }
    int main(int, char *[]) {}"
  NEEDS_REDUNDANT_MOVE
)

INCLUDE(CheckCXXSourceRuns)

CHECK_CXX_SOURCE_RUNS("
    #include<time.h>
    int main(int, char *[]) {
      time_t t = -1000;
      return gmtime(&t) == 0;
    }"
  HAS_PRE_1970
)

CHECK_CXX_SOURCE_RUNS("
    #include<stdlib.h>
    #include<time.h>
    int main(int, char *[]) {
      setenv(\"TZ\", \"America/Los_Angeles\", 1);
      tzset();
      struct tm time2037;
      struct tm time2038;
      strptime(\"2037-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2037);
      strptime(\"2038-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2038);
      return mktime(&time2038) - mktime(&time2037) != 31536000;
    }"
  HAS_POST_2038
)

CHECK_CXX_SOURCE_COMPILES("
    #include <initializer_list>
    struct A {
      A(std::initializer_list<int> list);
    };
    int main(int,char*[]){
    }"
  ORC_CXX_HAS_INITIALIZER_LIST
)

CHECK_CXX_SOURCE_COMPILES("
    int main(int,char*[]) noexcept {
      return 0;
    }"
  ORC_CXX_HAS_NOEXCEPT
)

CHECK_CXX_SOURCE_COMPILES("
    int main(int,char* argv[]){
      return argv[0] != nullptr;
    }"
  ORC_CXX_HAS_NULLPTR
)

CHECK_CXX_SOURCE_COMPILES("
    struct A {
      virtual ~A();
      virtual void foo();
    };
    struct B: public A {
      virtual void foo() override;
    };
    int main(int,char*[]){
    }"
  ORC_CXX_HAS_OVERRIDE
)

CHECK_CXX_SOURCE_COMPILES("
    #include<memory>
    int main(int,char* []){
      std::unique_ptr<int> ptr(new int);
    }"
  ORC_CXX_HAS_UNIQUE_PTR
)

CHECK_CXX_SOURCE_COMPILES("
    #include <cstdint>
    int main(int, char*[]) { }"
  ORC_CXX_HAS_CSTDINT
)

configure_file (
  "orc-config.hh.in"
  "${CMAKE_CURRENT_BINARY_DIR}/orc-config.hh"
)

configure_file (
  "Adaptor.hh.in"
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
)

include_directories (
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIR}
  ${SNAPPY_INCLUDE_DIR}
  ${LZ4_INCLUDE_DIR}
)

PROTOBUF_GENERATE_CPP(
  ORC_PROTO_SRCS ORC_PROTO_HDRS ORC_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/..
  PROTO_FILES orc_proto.proto
)

add_library (Orc #STATIC
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
  orc_proto.pb.h
  wrap/orc-proto-wrapper.cc
  ByteRLE.cc
  ColumnPrinter.cc
  ColumnReader.cc
  Compression.cc
  Exceptions.cc
  Int128.cc
  LzoDecompressor.cc
  MemoryPool.cc
  OrcFile.cc
  Reader.cc
  RLEv1.cc
  RLEv2.cc
  RLE.cc
  Statistics.cc
  StripeStream.cc
  Timezone.cc
  TypeImpl.cc
  Vector.cc
)

add_dependencies(Orc ${ORC_PROTO_TGTS})
