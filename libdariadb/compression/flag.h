#pragma once

#include "base_compressor.h"
#include "positions.h"

namespace dariadb {
    namespace compression {

        class FlagCompressor:public BaseCompressor {
        public:
            FlagCompressor() = default;
            FlagCompressor(const BinaryBuffer_Ptr &bw);
            ~FlagCompressor();

            bool append(Flag v);

            FlagCompressionPosition get_position()const;
            void restore_position(const FlagCompressionPosition&pos);
        protected:
            bool _is_first;
            Flag _first;
        };

        class FlagDeCompressor :public BaseCompressor {
        public:
            FlagDeCompressor() = default;
            FlagDeCompressor(const BinaryBuffer_Ptr &bw, Flag first);
            ~FlagDeCompressor() = default;

            Flag read();
        protected:
            Flag _prev_value;
        };

    }
}
