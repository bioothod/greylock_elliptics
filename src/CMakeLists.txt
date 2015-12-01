add_executable(greylock_test test.cpp)
target_link_libraries(greylock_test
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${LZ4_LIBRARIES}
)

add_executable(greylock_meta meta.cpp)
target_link_libraries(greylock_meta
	${Boost_LIBRARIES}
	${ELLIPTICS_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${LZ4_LIBRARIES}
)

add_executable(greylock_page_info page_info.cpp)
target_link_libraries(greylock_page_info
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

install(TARGETS	greylock_server greylock_test
	RUNTIME DESTINATION bin COMPONENT runtime)

