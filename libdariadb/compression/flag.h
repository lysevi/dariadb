#pragma once

#include "base_compressor.h"

namespace dariadb {
    namespace compression {

        class FlagCompressor:public BaseCompressor {
        public:
            FlagCompressor() = default;
            FlagCompressor(const BinaryBuffer &bw);
            ~FlagCompressor();

            bool append(Flag v);
        protected:
            bool _is_first;
            Flag _first;
        };

        class FlagDeCompressor :public BaseCompressor {
        public:
            FlagDeCompressor() = default;
            FlagDeCompressor(const BinaryBuffer &bw, Flag first);
            ~FlagDeCompressor() = default;

            Flag read();
        protected:
            Flag _prev_value;
        };

    }
}
