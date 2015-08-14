add_executable(greylock_test index.cpp)
target_link_libraries(greylock_test
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${LZ4_LIBRARIES}
)

add_executable(greylock_server server.cpp)
target_link_libraries(greylock_server
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${THEVOID_LIBRARIES}
	${SWARM_LIBRARIES}
	${LZ4_LIBRARIES}
)
