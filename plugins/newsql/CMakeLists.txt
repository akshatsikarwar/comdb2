include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(
  ${SQLITE_FLAGS}
)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/lua
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/sqlite
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
  ${CapnProto_INCLUDE_DIRS}
)
set(NEWSQL_SRCS
  newsql.c
  newsql_evbuffer.c
  ${PROJECT_SOURCE_DIR}/protobuf/capnp_codec.cpp
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.capnp.c++
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.capnp.c++
)
set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.capnp.c++
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.capnp.c++
  PROPERTIES GENERATED TRUE
)
add_plugin(newsql STATIC "${NEWSQL_SRCS}")
add_dependencies(newsql sqlite capnp_schemas) # Wait for parse.h and capnp headers
target_link_libraries(newsql PUBLIC net CapnProto::capnp)
if (COMDB2_BBCMAKE)
  comdb2_lib_target(newsql)
endif()
