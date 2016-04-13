#pragma once

#include <ostream>
#include "../utils/exception.h"
#include "../utils/utils.h"
#include <memory>

namespace dariadb {
    namespace compression {

        const uint8_t max_bit_pos = 7;

        class BinaryBuffer;
        typedef std::shared_ptr<BinaryBuffer> BinaryBuffer_Ptr;
        class BinaryBuffer {

        public:
            BinaryBuffer(const utils::Range& r);
            BinaryBuffer() = default;
            ~BinaryBuffer();
            BinaryBuffer(const BinaryBuffer &other);
            BinaryBuffer(BinaryBuffer &&other);
            BinaryBuffer &operator=(const BinaryBuffer &other);
            BinaryBuffer &operator=(const BinaryBuffer &&other);

            void swap(BinaryBuffer &other) noexcept;

			int8_t bitnum() const { return _bitnum; }
            size_t pos() const { return _pos; }

            void set_bitnum(int8_t num);
            void set_pos(size_t pos);
            void reset_pos();

            BinaryBuffer &incbit();
            BinaryBuffer &incpos();

            size_t cap() const { return _cap; }
            size_t free_size() const { return _pos; }
            bool is_full() const { return _pos == 0; }

            uint8_t getbit() const;
            BinaryBuffer &setbit();
            BinaryBuffer &clrbit();

            friend std::ostream &operator<<(std::ostream &stream, const BinaryBuffer &b);

            void write(uint16_t v, int8_t count);
            void write(uint64_t v, int8_t count);
            uint64_t read(int8_t count);

            //TODO move to cpp
            dariadb::utils::Range get_range()const {return  dariadb::utils::Range{_begin,_end};}
        protected:
            inline void move_pos(int8_t count) {
                if (count < _bitnum) {
                    _bitnum -= count + 1;
                } else {
                    int n = count - _bitnum;
                    int r = 1 + (n >> 3);
                    _bitnum = max_bit_pos - (n & 7);
                    _pos -= r;
                }

                if (_pos > _cap) {
                    throw MAKE_EXCEPTION("BinaryBuffer::move_pos");
                }
            }

        protected:
            uint8_t *_begin, *_end;
            size_t _cap;

            size_t _pos;
            int8_t _bitnum;
        };

        std::ostream &operator<<(std::ostream &stream, const BinaryBuffer &b);
    }
}
