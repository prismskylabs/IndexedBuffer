#add_executable(buffer-test
#buffer-test.cpp)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${BOOSTFILESYSTEM_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

#target_link_libraries(buffer-test
#${GTEST_BOTH_LIBRARIES}
#${INDEXEDBUFFER_LIBRARIES})

#add_test(NAME buffer-test COMMAND buffer-test)

#add_executable(database-test
#database-test.cpp)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${BOOSTFILESYSTEM_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIRS}
    ${SQLITE_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

#target_link_libraries(database-test
#${GTEST_BOTH_LIBRARIES}
#${INDEXEDBUFFER_LIBRARIES})

#add_test(NAME database-test COMMAND database-test)

add_executable(chrono-snap-test
    chrono-snap-test.cpp)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${GTEST_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

target_link_libraries(chrono-snap-test
    ${GTEST_BOTH_LIBRARIES}
    ${BOOSTFILESYSTEM_LIBRARIES})

add_test(NAME chrono-snap-test COMMAND chrono-snap-test)

#add_executable(filesystem-test
#filesystem-test.cpp)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${BOOSTFILESYSTEM_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIRS}
    ${INDEXEDBUFFER_INCLUDE_DIRS})

#target_link_libraries(filesystem-test
#${GTEST_BOTH_LIBRARIES}
#${INDEXEDBUFFER_LIBRARIES})

#add_test(NAME filesystem-test COMMAND filesystem-test)
