get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

aux_source_directory(${SRC_DIR} SRC_LIST)

set(src_transaction ${SRC_LIST} CACHE INTERNAL "Transaction source files")
