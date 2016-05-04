Get-ChildItem . -name -recurse *.cpp | %{clang-format -style=file -i $_}
Get-ChildItem . -name -recurse *.h | %{clang-format -style=file -i $_}