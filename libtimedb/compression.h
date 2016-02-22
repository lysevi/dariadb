#pragma once

#include <ostream>
#include "meas.h"

namespace timedb {
	namespace compression {

        const uint8_t max_bit_pos = 7;


        class BinaryWriter {
             friend std::ostream& operator<< (std::ostream& stream, const BinaryWriter& b);
		public:
            BinaryWriter(uint8_t* _begin,uint8_t*_end);
            BinaryWriter(const BinaryWriter&other);
            BinaryWriter(BinaryWriter&&other);
            ~BinaryWriter();
            BinaryWriter& operator=(const BinaryWriter&other);

            void swap(BinaryWriter &other) throw();

            int bitnum()const { return _bitnum; }
			size_t pos()const { return _pos; }
			
			void set_bitnum(size_t num);
			void set_pos(size_t pos);
			void reset_pos();

            BinaryWriter& incbit();
            BinaryWriter& incpos();
			
			size_t cap()const { return _cap;}

			uint8_t getbit()const;
            BinaryWriter& setbit();
            BinaryWriter& clrbit();


        protected:
            uint8_t* _begin,*_end;
			size_t   _cap;

			size_t _pos;
            int _bitnum;
		};

        std::ostream& operator<< (std::ostream& stream, const BinaryWriter& b){
            stream<<" pos:"<<b._pos<<" cap:"<<b._cap<<" bit:"<<b._bitnum<<" [";
            for(size_t i=0;i<=b._pos;i++){
                for(int bit=int(max_bit_pos);bit>=0;bit--){
                    auto is_cur=((bit==b._bitnum) && (i==b._pos));
                    if (is_cur)
                        stream<<"[";
                    stream <<((b._begin[i] >> bit) & 1);
                    if (is_cur)
                        stream<<"]";
                    if(bit==4) stream<<" ";
                }
                stream<<std::endl;
            }
            return stream<<"]";
        }

        class DeltaCompressor
		{
		public:
            DeltaCompressor()=default;
            DeltaCompressor(const BinaryWriter &bw);
			~DeltaCompressor();

            void append(Time t);

            static uint16_t get_delta_64(int64_t D);
            static uint16_t get_delta_256(int64_t D);
            static uint16_t get_delta_2048(int64_t D);
            static uint64_t get_delta_big(int64_t D);
        protected:
            bool _is_first;
            BinaryWriter _bw;
            Time  _first;
            uint64_t _prev_delta;
            Time _prev_time;
		};

        class DeltaDeCompressor
        {
        public:
            DeltaDeCompressor()=default;
            DeltaDeCompressor(const BinaryWriter &bw, Time first);
            ~DeltaDeCompressor();

            Time read();
        protected:
            BinaryWriter _bw;
            uint64_t _prev_delta;
            Time _prev_time;
        };

        class XorCompressor
        {
        public:
            XorCompressor()=default;
            XorCompressor(const BinaryWriter &bw);
            ~XorCompressor();

            void append(Value v);
            uint8_t zeros_lead(Value v);
            uint8_t zeros_tail(Value v);
        protected:
            bool _is_first;
            BinaryWriter _bw;
            Value  _first;
            Value _prev_value;
        };
	}
}
