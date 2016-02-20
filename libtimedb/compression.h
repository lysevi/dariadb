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

			void incbit();
			void incpos();
			
			size_t cap()const { return _cap;}

			uint8_t getbit()const;
			void setbit();
			void clrbit();


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
			DeltaCompressor();
			~DeltaCompressor();

			static uint16_t get_delta_64(uint64_t D);
			static uint16_t get_delta_256(uint64_t D);
			static uint16_t get_delta_2048(uint64_t D);
			static uint64_t get_delta_big(uint64_t D);
		};
	}
}
