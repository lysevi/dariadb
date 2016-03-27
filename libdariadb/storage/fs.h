#pragma once

#include <string>
#include <list>

namespace dariadb{
    namespace  storage {
        namespace fs {
            std::list<std::string> ls(const std::string &path);
            std::list<std::string> ls(const std::string &path, const std::string &ext);

            bool rm(const std::string &rm_path);
            std::string filename(std::string fname); // without ex
            std::string parent_path(std::string fname);

        }
    }
}
