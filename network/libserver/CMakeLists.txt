FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB CMN_HDRS "../common/*.h")
FILE(GLOB CMN_SRC "../common/*.cpp")

FILE(GLOB CMN_PROTO "../common/messages/*.proto")

SET(CMD protobuf::protoc)
if(SYSTEM_PROTOBUF)
  SET(CMD ${Protobuf_PROTOC_EXECUTABLE})
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

if(MSVC)
        add_library(libnet-server STATIC ${SRC})
else()
        add_library(libnet-server SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-server libdariadb ${Boost_LIBRARIES})
endif()

if(SYSTEM_PROTOBUF) #Legacy mode
    target_link_libraries(libnet-server ${PROTOBUF_LIBRARIES})
else()
    target_link_libraries(libnet-server protobuf::libprotobuf)
endif()

IF(WIN32)
 target_link_libraries(libnet-server wsock32 ws2_32)
endif(WIN32)
