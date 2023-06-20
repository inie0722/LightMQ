add_executable(fixed_table EXCLUDE_FROM_ALL fixed_table.cpp)
add_executable(variable_table EXCLUDE_FROM_ALL variable_table.cpp)

target_link_libraries(fixed_table GTest::gtest)
target_link_libraries(variable_table GTest::gtest)

add_test(NAME fixed_table COMMAND fixed_table)
add_test(NAME variable_table COMMAND variable_table)