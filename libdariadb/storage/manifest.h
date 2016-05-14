#pragma once

#include <string>
#include <list>

namespace dariadb{
    namespace storage{
class Manifest{
public:
    Manifest(const std::string&fname);
    std::list<std::string> list();
    void append(const std::string&rec);
protected:
    std::string _filename;
};
    }
}
