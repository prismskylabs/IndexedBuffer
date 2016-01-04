add_library(${INDEXEDBUFFER_LIBRARIES} STATIC
    buffer.cpp
    chrono-snap.cpp
    database.cpp
    filesystem.cpp
    ${INDEXEDBUFFER_INCLUDE_DIRS}/indexed/buffer.h
    ${INDEXEDBUFFER_INCLUDE_DIRS}/indexed/chrono-snap.h
    ${INDEXEDBUFFER_INCLUDE_DIRS}/indexed/database.h
    ${INDEXEDBUFFER_INCLUDE_DIRS}/indexed/filesystem.h)

target_include_directories(${INDEXEDBUFFER_LIBRARIES} PRIVATE
    ${INDEXEDBUFFER_INCLUDE_DIRS}
    ${SQLITE_INCLUDE_DIRS}
    ${BOOSTFILESYSTEM_INCLUDE_DIRS})

target_link_libraries(${INDEXEDBUFFER_LIBRARIES}
    ${SQLITE_LIBRARIES}
    ${BOOSTFILESYSTEM_LIBRARIES})
