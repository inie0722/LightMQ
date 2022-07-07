add_executable(fixed_table EXCLUDE_FROM_ALL fixed_table.cpp)

set(target_lib GTest::gtest)

target_link_libraries(fixed_table ${target_lib})

add_test(NAME fixed_table COMMAND fixed_table)