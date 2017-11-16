add_executable(cdb2rcv cdb2rcv.cpp)
add_definitions(-DSQLITE_BKUP)
include_directories(
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/cson
  ${OPENSSL_INCLUDE_DIR}
)
set(libs
  cdb2api
  cson
  ${PROTOBUF_C_LIBRARY}
  ${SQLITE3_LIBRARY}
)
if(WITH_SSL)
  list(APPEND libs ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endif()
target_link_libraries(cdb2rcv ${libs})
#install(TARGETS cdb2rcv RUNTIME DESTINATION bin)
