include(GenerateExportHeader)
FILE(GLOB CMN_HDRS "*.h")
FILE(GLOB CMN_SRC "*.cpp")

FILE(GLOB CMN_PROTO "messages/*.proto")

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

SET(SRC ${CMN_HDRS} ${CMN_SRC})

add_library(libcommon-net SHARED ${SRC})
if(CMAKE_COMPILER_IS_GNUCXX)
  MESSAGE(STATUS "Enable -fPIC for libcommon-net")
  SET_TARGET_PROPERTIES(libcommon-net PROPERTIES COMPILE_FLAGS "-fPIC")
endif()

if(SYSTEM_PROTOBUF) #Legacy mode
    target_link_libraries(libcommon-net ${PROTOBUF_LIBRARIES})
else()
    target_link_libraries(libcommon-net protobuf::libprotobuf)
endif()

IF(WIN32)
 target_link_libraries(libcommon-net wsock32 ws2_32)
endif(WIN32)
 
 target_link_libraries(libcommon-net libdariadb)

GENERATE_EXPORT_HEADER(libcommon-net
    BASE_NAME libcommon-net
    EXPORT_MACRO_NAME DARIADBNET_CMN_EXPORTS
    EXPORT_FILE_NAME dariadb_net_cmn_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)