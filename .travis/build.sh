#!/bin/bash

echo "lcov --directory . --zerocounters"
lcov --directory . --zerocounters

if [ "$CLANG" == "TRUE" ]; then
    echo "clang compiller. " `clang --version`
    export CC="clang"
    export CXX="clang"

    if [[ "${CLANG_SANITIZER}" == "ASAN_UBSAN" ]]; then
        cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" 	-DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} -lstdc++"  -DCLANG_ASAN_UBSAN=ON  .
    fi

    if [[ "${CLANG_SANITIZER}" == "MSAN" ]]; then
        cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" 	-DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} -lstdc++"  -DCLANG_MSAN=ON  .
    fi

    if [[ -z "${CLANG_SANITIZER}" ]]; then
    	cmake  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} -lstdc++"  .
    fi
fi

if [ "$CLANG" == "FALSE" ]; then
    echo "gcc compiller " `gcc-6 --version`
    export CC="gcc-6"
    export CXX="g++-6"
    if [[ "$GCOV" == "TRUE" ]]; then
       echo "enable test coverage..."
       cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} --coverage -O0" \
	    -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS}"  .
    else
       cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE}" \
	    -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS}"  .
    fi
fi

if [ $? -ne 0 ]; then
    exit 1
fi

make -j2

if [ $? -ne 0 ]; then
    exit 1
fi

#ctest --output-on-failure .
ctest -VV .

if [ $? -ne 0 ]; then
    exit 1
fi

if [[ "$GCOV" == "TRUE" ]]; then
    echo "cd ${TRAVIS_BUILD_DIR}"
    cd ${TRAVIS_BUILD_DIR}
    echo "lcov --directory . --capture --output-file coverage.info"
    lcov --directory . --capture --output-file coverage.info # capture coverage info
    echo "lcov --remove coverage.info 'bin/*' 'tests/*' 'extern/*' 'benchmarks/*' '/usr/*'"
    lcov --remove coverage.info 'bin/*' 'tests/*' 'extern/*' 'benchmarks/*' '/usr/*' --output-file coverage.info # filter out system and test code
    echo "lcov --list coverage.info"    
    lcov --list coverage.info # debug before upload
    coveralls-lcov --repo-token '7VSWJleC3m9GKbZBakLFL5nBEib1CTFsb' coverage.info 
fi
