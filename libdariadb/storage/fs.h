#pragma once

#include "../utils.h"
#include <string>
#include <list>
#include <memory>

namespace dariadb{
    namespace  utils {
        namespace fs {
            std::list<std::string> ls(const std::string &path);
            std::list<std::string> ls(const std::string &path, const std::string &ext);

            bool rm(const std::string &rm_path);
            std::string filename(std::string fname); // without ex
            std::string parent_path(std::string fname);

			bool path_exists(const std::string&path);
			void mkdir(const std::string&path);

            class MappedFile:public utils::NonCopy{
                class Impl;
                MappedFile(Impl* im);
            public:
                using MapperFile_ptr=std::shared_ptr<MappedFile>;

                ~MappedFile();
                void close();
                uint8_t* data();

                static MapperFile_ptr open(const std::string&path);
                static MapperFile_ptr touch(const std::string&path, uint64_t size);
            private:
                std::unique_ptr<Impl> _impl;
            };

        }
    }
}
