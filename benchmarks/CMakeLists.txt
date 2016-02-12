include_directories(../libtimedb)

add_executable (b1 b1.cpp)
TARGET_LINK_LIBRARIES(b1 libtimedb ${Boost_LIBRARIES})