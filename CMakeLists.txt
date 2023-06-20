cmake_minimum_required(VERSION 3.12)
project(AirLightmdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

include_directories(${CMAKE_SOURCE_DIR}/include)

if(Test)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    add_subdirectory(${CMAKE_SOURCE_DIR}/test)
endif()

add_library(${PROJECT_NAME} INTERFACE)

install(TARGETS ${PROJECT_NAME} EXPORT ${PROJECT_NAME}Config)
install(EXPORT ${PROJECT_NAME}Config
        FILE ${PROJECT_NAME}Config.cmake
        DESTINATION share/${PROJECT_NAME}
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
    	DESTINATION include
        FILES_MATCHING
        PATTERN *.hpp
)