include_directories(../libdariadb)
MESSAGE(STATUS "Tools: ")

macro(ADD_TOOL name src)
        MESSAGE(STATUS " +" ${name})
        add_executable(${name} ${src})

        target_link_libraries(${name}
                                ${CMAKE_THREAD_LIBS_INIT}
                                ${Boost_LIBRARIES}
                                libdariadb)

         set_target_properties(${name} PROPERTIES FOLDER Tools)
endmacro(ADD_TOOL)

ADD_TOOL(page_info page_info.cpp)
