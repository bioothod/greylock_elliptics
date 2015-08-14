cmake_minimum_required (VERSION 2.6)
project (greylock)

#FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)

#string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
#STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" WOOKIE_VERSION_MAJOR "${DEBFULLVERSION}")
#STRING (REGEX MATCH "([0-9]+$)" WOOKIE_VERSION_MINOR "${DEBFULLVERSION}")

set(CMAKE_CXX_FLAGS "-g -std=c++0x -W -Wall -Wextra -fstack-protector-all")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

find_package(Boost REQUIRED COMPONENTS system program_options)
find_package(Elliptics REQUIRED)

find_path(RIBOSOME_INCLUDE_DIRS NAMES "ribosome/timer.hpp" PATH_SUFFIXES "ribosome")
if (NOT RIBOSOME_INCLUDE_DIRS)
	message(FATAL_ERROR "Ribosome development files have not been found")
endif()
message("Ribosome headers: ${RIBOSOME_INCLUDE_DIRS}")

INCLUDE(cmake/locate_library.cmake)

LOCATE_LIBRARY(MSGPACK "msgpack.hpp" "msgpack")
LOCATE_LIBRARY(THEVOID "thevoid/server.hpp" "thevoid")
LOCATE_LIBRARY(SWARM "swarm/http_request.hpp" "swarm")
LOCATE_LIBRARY(LZ4 "lz4frame.h" "lz4")

FILE(GLOB headers
	"${CMAKE_CURRENT_SOURCE_DIR}/include/greylock/*.hpp"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/greylock/*.h"

)
install(FILES ${headers} DESTINATION include/greylock)

include_directories(${PROJECT_SOURCE_DIR}/include
	${MSGPACK_INCLUDE_DIRS}
	${ELLIPTICS_INCLUDE_DIRS}
	${RIBOSOME_INCLUDE_DIRS}
	${THEVOID_INCLUDE_DIRS}
	${SWARM_INCLUDE_DIRS}
	${LZ4_INCLUDE_DIRS}
)

link_directories(
	${Boost_LIBRARY_DIRS}
	${MSGPACK_LIBRARY_DIRS}
	${ELLIPTICS_LIBRARY_DIRS}
	${RIBOSOME_LIBRARY_DIRS}
	${THEVOID_LIBRARY_DIRS}
	${SWARM_LIBRARY_DIRS}
	${LZ4_LIBRARY_DIRS}
)

add_subdirectory(src)
