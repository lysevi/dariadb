include(GenerateExportHeader)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB CMN_HDRS "../common/*.h")
FILE(GLOB CMN_SRC "../common/*.cpp")

FILE(GLOB CMN_PROTO "../common/messages/*.proto")

SET(CMD protobuf::protoc)
if(SYSTEM_PROTOBUF)
  SET(CMD protoc)
endif()

#Code Generation
if(protobuf_MODULE_COMPATIBLE) #Legacy Support
   protobuf_generate_cpp(common_PROTOC_SRCS common_PROTO_HDRS ${CMN_PROTO})
   list(APPEND CMN_SRC ${common_PROTOC_SRCS})
   list(APPEND CMN_HDRS ${common_PROTO_HDRS})
else()

  foreach(proto_file ${CMN_PROTO})
       get_filename_component(proto_file_abs ${proto_file} ABSOLUTE)
        get_filename_component(basename ${proto_file} NAME_WE)
		get_filename_component(proto_dir ${proto_file_abs} DIRECTORY  )
        set(generated_files ${basename}.pb.cc ${basename}.pb.h)
        list(APPEND CMN_SRC ${generated_files})
		        
		add_custom_command(
            OUTPUT ${generated_files}
            COMMAND ${CMD}
            ARGS --cpp_out ${CMAKE_CURRENT_BINARY_DIR} -I ${proto_dir} ${proto_file_abs} 
            COMMENT "Generating ${generated_files} from ${proto_file}"
            VERBATIM)
  endforeach()
endif(protobuf_MODULE_COMPATIBLE)

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${CMN_HDRS} ${CMN_SRC})

source_group(common FILES ${CMN_HDRS} ${CMN_SRC})

add_library(libdariadb-cl SHARED ${SRC})
TARGET_LINK_LIBRARIES(libdariadb-cl libdariadb ${Boost_LIBRARIES})

if(SYSTEM_PROTOBUF) #Legacy mode
    target_link_libraries(libdariadb-cl ${PROTOBUF_LIBRARIES})
    SET(DARIADB_CLIENT_LIBS libdariadb-cl ${Boost_LIBRARIES} ${PROTOBUF_LIBRARIES} CACHE STRING "Dariadb libs for build client.")
else()
    target_link_libraries(libdariadb-cl protobuf::libprotobuf)
    SET(DARIADB_CLIENT_LIBS libdariadb-cl ${Boost_LIBRARIES}  protobuf::libprotobuf CACHE STRING "Dariadb libs for build client.")
endif()

GENERATE_EXPORT_HEADER(libdariadb-cl           
    BASE_NAME libdariadb-cl
    EXPORT_MACRO_NAME DARIADBCL_EXPORTS
    EXPORT_FILE_NAME dariadb_cl_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)

IF(WIN32)
 target_link_libraries(libdariadb-cl wsock32 ws2_32)
endif(WIN32)