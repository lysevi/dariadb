#pragma once

#include "binarybuffer.h"
#include "../meas.h"

namespace memseries {
    namespace compression {

        class DeltaCompressor {
        public:
            DeltaCompressor() = default;
            DeltaCompressor(const BinaryBuffer &bw);
            ~DeltaCompressor();

            bool append(Time t);

            static uint16_t get_delta_64(int64_t D);
            static uint16_t get_delta_256(int64_t D);
            static uint16_t get_delta_2048(int64_t D);
            static uint64_t get_delta_big(int64_t D);

            bool is_full() const { return _bw.is_full(); }

            size_t writed()const{return _bw.cap()-_bw.pos();}
        protected:
            bool _is_first;
            BinaryBuffer _bw;
            Time _first;
            uint64_t _prev_delta;
            Time _prev_time;
        };

        class DeltaDeCompressor {
        public:
            DeltaDeCompressor() = default;
            DeltaDeCompressor(const BinaryBuffer &bw, Time first);
            ~DeltaDeCompressor();

            Time read();

            bool is_full() const { return _bw.is_full(); }

        protected:
            BinaryBuffer _bw;
            int64_t _prev_delta;
            Time _prev_time;
        };
    }
}
