#pragma once

#include "binarybuffer.h"
#include "../meas.h"

namespace memseries {
    namespace compression {

        class FlagCompressor {
        public:
            FlagCompressor() = default;
            FlagCompressor(const BinaryBuffer &bw);
            ~FlagCompressor();

            bool append(Flag v);
            bool is_full() const { return _bw.is_full(); }
            size_t writed()const{return _bw.cap()-_bw.pos();}
        protected:
            BinaryBuffer _bw;
            bool _is_first;
            Flag _first;
        };

        class FlagDeCompressor {
        public:
            FlagDeCompressor() = default;
            FlagDeCompressor(const BinaryBuffer &bw, Flag first);
            ~FlagDeCompressor() = default;

            Flag read();

            bool is_full() const { return _bw.is_full(); }

        protected:
            BinaryBuffer _bw;
            Flag _prev_value;
        };

    }
}
