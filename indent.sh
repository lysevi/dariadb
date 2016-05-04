#!/bin/sh
find . -name '*.h' -exec clang-format --style=file -i {} \;
find . -name '*.cpp' -exec clang-format --style=file -i {} \;
