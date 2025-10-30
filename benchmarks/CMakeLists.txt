add_executable(index_perftest index_perftest.cpp)
target_link_libraries(index_perftest
  pisa
)

add_executable(perftest_interpolative perftest_interpolative.cpp)
target_link_libraries(perftest_interpolative
  pisa
)

add_executable(perftest_interpolative_encoded_size perftest_interpolative_encoded_size.cpp)
target_link_libraries(perftest_interpolative_encoded_size
  pisa
)

add_executable(scan_perftest scan_perftest.cpp)
target_link_libraries(scan_perftest
  pisa
)
