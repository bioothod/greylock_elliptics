cmake_minimum_required (VERSION 2.6)
project (indexes)

#FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)

#string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
#STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" WOOKIE_VERSION_MAJOR "${DEBFULLVERSION}")
#STRING (REGEX MATCH "([0-9]+$)" WOOKIE_VERSION_MINOR "${DEBFULLVERSION}")

set(CMAKE_CXX_FLAGS "-g -std=c++0x -W -Wall -Wextra -fstack-protector-all")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(Boost REQUIRED COMPONENTS system program_options)
find_package(Elliptics REQUIRED)

find_path(RIBOSOME_INCLUDE_DIRS NAMES "ribosome/timer.hpp" PATH_SUFFIXES "ribosome")
if (NOT RIBOSOME_INCLUDE_DIRS)
	message(FATAL_ERROR "Ribosome development files have not been found")
endif()
message("Ribosome headers: ${RIBOSOME_INCLUDE_DIRS}")

INCLUDE(cmake/locate_library.cmake)

LOCATE_LIBRARY(MSGPACK "msgpack.hpp" "msgpack")

FILE(GLOB headers
	"${CMAKE_CURRENT_SOURCE_DIR}/include/indexes/*.hpp"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/indexes/*.h"

)
install(FILES ${headers} DESTINATION include/indexes)

include_directories(${PROJECT_SOURCE_DIR}/include
	${MSGPACK_INCLUDE_DIRS}
	${ELLIPTICS_INCLUDE_DIRS}
	${RIBOSOME_INCLUDE_DIRS}
)

link_directories(
	${Boost_LIBRARY_DIRS}
	${MSGPACK_LIBRARY_DIRS}
	${ELLIPTICS_LIBRARY_DIRS}
	${RIBOSOME_LIBRARY_DIRS}
)

add_subdirectory(src)
