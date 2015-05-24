add_executable(indexes_test index.cpp)
target_link_libraries(indexes_test
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
)
