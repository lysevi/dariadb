#pragma once

#include "../meas.h"
#include "binarybuffer.h"

namespace memseries {
    namespace compression {

        namespace inner {
            union conv {
                double d;
                int64_t i;
            };

            inline int64_t FlatDouble2Int(double v) {
                conv c;
                c.d = v;
                return c.i;
            }
            inline double FlatInt2Double(int64_t v) {
                conv c;
                c.i = v;
                return c.d;
            }
        }

        class XorCompressor {
        public:
            XorCompressor() = default;
            XorCompressor(const BinaryBuffer &bw);
            ~XorCompressor();
            XorCompressor(const XorCompressor &other);
            void swap(XorCompressor &other);
            XorCompressor &operator=(XorCompressor &other);
            XorCompressor &operator=(XorCompressor &&other);

            bool append(Value v);
            static uint8_t zeros_lead(uint64_t v);
            static uint8_t zeros_tail(uint64_t v);

            bool is_full() const { return _bw.is_full(); }

        protected:
            bool _is_first;
            BinaryBuffer _bw;
            uint64_t _first;
            uint64_t _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };

        class XorDeCompressor {
        public:
            XorDeCompressor() = default;
            XorDeCompressor(const BinaryBuffer &bw, Value first);
            ~XorDeCompressor() = default;
            XorDeCompressor(const XorDeCompressor &other);
            void swap(XorDeCompressor &other);
            XorDeCompressor &operator=(XorDeCompressor &other);
            XorDeCompressor &operator=(XorDeCompressor &&other);

            Value read();

            bool is_full() const { return _bw.is_full(); }

        protected:
            BinaryBuffer _bw;
            uint64_t _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };
    }
}
