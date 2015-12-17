add_executable(filesystem-test
    filesystem-test.cpp)

target_include_directories(filesystem-test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${BOOSTFILESYSTEM_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

target_link_libraries(filesystem-test
    ${GTEST_BOTH_LIBRARIES}
    ${INDEXEDBUFFER_LIBRARIES})

add_test(NAME filesystem-test COMMAND filesystem-test)

add_executable(chrono-snap-test
    chrono-snap-test.cpp)

target_include_directories(chrono-snap-test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${GTEST_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

target_link_libraries(chrono-snap-test
    ${GTEST_BOTH_LIBRARIES}
    ${INDEXEDBUFFER_LIBRARIES})

add_test(NAME chrono-snap-test COMMAND chrono-snap-test)
