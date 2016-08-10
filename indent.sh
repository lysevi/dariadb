#!/bin/sh
find . -name '*.h' -exec clang-format-3.8 --style=file -i {} \;
find . -name '*.cpp' -exec clang-format-3.8 --style=file -i {} \;
