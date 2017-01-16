# Compressing time stamps

1. The block header stores the starting time stamp, 
2. For subsequent time stamps, tn:
 * Calculate the delta of delta: D = (tn - t(n-1)) 
 * If D is [-32:32], store '10' followed by the value.
 * If D is between [-4096:4096], store '1110' followed by the value.
 * if D is between [-524288:524287], store '1110' followed by the value.
 * Otherwise store '0' byte followed by D using 64 bits

# Compressing values

We then encode these XOR d values with the following variable length encoding scheme:
1. The first value is stored with no compression
2. If XOR with the previous is zero (same value), store  single '0' byte
3. When XOR is non-zero, storing byte with count of bytes with meaningful bits. 
Then storing count of zeros in tail(byte), then XOR moved 
to the right (for example '0001010 => 0000101').

```C++
  auto lead = dariadb::utils::clz(xor_val);
  auto tail = dariadb::utils::ctz(xor_val);
  const size_t total_bits = sizeof(Value) * 8;
  
  uint8_t count_of_bytes=(total_bits - lead - tail)/8+1; //count of byte in XOR
  assert(count_of_bytes <= u64_buffer_size);

  bw->write(count_of_bytes);
  bw->write(tail);
  
  auto moved_value = xor_val >> tail; // 0001010 => 0000101
  uint8_t buff[u64_buffer_size];
  *reinterpret_cast<uint64_t*>(buff) = moved_value;
  for (size_t i = 0; i < count_of_bytes; ++i) {
	   bw->write(buff[i]);
  }
```

# Flags

Using LEB128 compression;
