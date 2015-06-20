add_executable(indexes_test index.cpp)
target_link_libraries(indexes_test
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${LZ4_LIBRARIES}
)

add_executable(indexes_server server.cpp)
target_link_libraries(indexes_server
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${THEVOID_LIBRARIES}
	${SWARM_LIBRARIES}
	${LZ4_LIBRARIES}
)
