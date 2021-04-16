// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression.h"

#include <cstdint>
#include <sstream>
#include <iostream>

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

namespace {
// ----------------------------------------------------------------------
// Block-based LZO codec implementation, where blocks are encoded based on Hadoop
// BlockCompressorStream (src/core/org/apache/hadoop/io/compress/BlockCompressorStream.java).
// Each block starts with 4-byte length of the original uncompressed block, followed by one
// or more chunks. Where each chunk is prefixed with 4-byte length of the compressed block.
static const uint32_t kMaxCompressedChunkSize = 256 * 1024;

// LZO codec.
class LzoCodec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output) override {
    if (lzo_init() != LZO_E_OK) {
      return Status::IOError("Failed to initialize LZO.");
    }
    int64_t input_offset = 0;
    int64_t output_offset = 0;
    while (input_offset < input_len) {
      // First 4 bytes of the compressed block is the size of the original uncompressed
      // block that we are decompressing.
      const int64_t orig_block_size = readInt32(input, input_offset);
      int64_t initial_output_offset = output_offset;

      // Keep decompressing each chunk until we read the entire uncompressed block.
      while ((output_offset - initial_output_offset) < orig_block_size) {
        DCHECK(input_offset < input_len);
        DCHECK(output_offset < output_buffer_len);
        Status status = decompressNextChunk(input, input_offset, input_len, output,
                                          output_offset, output_buffer_len);
        if (!status.ok()) {
          return status;
        }
      }
    }
    DCHECK(output_offset <= output_buffer_len);
    DCHECK(input_offset == input_len);
    return output_offset;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return input_len + getCompressionOverhead(input_len);
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    if (lzo_init() != LZO_E_OK) {
      return Status::IOError("Failed to initialize LZO.");
    }
    int64_t input_offset = 0;
    int64_t output_offset = 0;

    // Write the size of the original uncompressed block.
    writeInt32(input_len, output_buffer, output_offset);

    // Allocate work-memory for LZO.
    std::unique_ptr<uint8_t[]> wrkmem(new uint8_t[LZO1X_1_MEM_COMPRESS]());
    if (!wrkmem.get()) {
      return Status::IOError("Failed to allocate LZO work-memory.");
    }

    // Go through the input buffer and compress each chunk based on maxChunkSize.
    while (input_offset < input_len) {
      Status status = compressChunk(input, input_offset, input_len, output_buffer, output_offset,
                                    output_buffer_len, wrkmem.get());
      if (!status.ok()) {
        return status;
      }
    }
    return output_offset;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented(
        "Streaming compression unsupported with LZO format. ");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented(
        "Streaming decompression unsupported with LZO format. ");
  }

  Compression::type compression_type() const override {
    return Compression::type::LZO;
  }

private:
  void writeInt32(int32_t value, uint8_t* buffer, int64_t& offset) {
    uint8_t* write_buffer = buffer + offset;
    offset += 4;
    write_buffer[0] = (value >> 24) & 0xFF;
    write_buffer[1] = (value >> 16) & 0xFF;
    write_buffer[2] = (value >> 8) & 0xFF;
    write_buffer[3] = (value >> 0) & 0xFF;
  }

  int32_t readInt32(const uint8_t* buffer, int64_t& offset) {
    const uint8_t* read_buffer = buffer + offset;
    offset += 4;
    return ((read_buffer[0] << 24) +
            (read_buffer[1] << 16) +
            (read_buffer[2] << 8) +
            (read_buffer[3] << 0));
  }

  Status decompressNextChunk(const uint8_t* input, int64_t& input_offset,
                             const int64_t input_len, uint8_t* output,
                             int64_t& output_offset, const int64_t output_len) {
    // Each chunk is prefixed with 4-byte integer, which represents the compressed size
    // of the chunk.
    const int64_t compressed_chunk_size = readInt32(input, input_offset);
    if (input_offset + compressed_chunk_size > input_len) {
      return Status::IOError("LZO decompress failed. Corrupted compression block size.");
    }
    const uint8_t* input_buffer = input + input_offset;
    uint8_t* output_buffer = output + output_offset;
    lzo_uint output_buffer_len = output_len - output_offset;
    int r = lzo1x_decompress_safe(input_buffer, compressed_chunk_size,
                                  output_buffer, &output_buffer_len,
                                  NULL);
    if (r != LZO_E_OK) {
      return Status::IOError("LZO decompress failed. Corrupted stream.");
    }
    input_offset += compressed_chunk_size;
    output_offset += output_buffer_len;
    return Status::OK();
  }

  Status compressChunk(const uint8_t* input, int64_t& input_offset,
                       const int64_t input_len, uint8_t* output,
                       int64_t& output_offset, const int64_t output_len,
                       uint8_t* wrkmem) {
    // Figure out the max compressed chunk size based on compression overhead.
    static const int64_t max_chunk_size =
      kMaxCompressedChunkSize - getCompressionOverhead(kMaxCompressedChunkSize);

    const uint8_t* input_buffer = input + input_offset;
    const uint32_t input_buffer_len = std::min(input_len - input_offset, max_chunk_size);
    DCHECK(input_offset + input_buffer_len <= input_len);

    // We reserve 4 bytes in the output buffer to write the size of the compressed chunk.
    uint8_t* output_buffer = output + output_offset + 4;
    lzo_uint written_buffer_len = output_len - output_offset - 4;

    // Do the LZO compression.
    int r = lzo1x_1_compress(input_buffer, input_buffer_len, output_buffer, &written_buffer_len,
                             wrkmem);
    if (r != LZO_E_OK) {
      return Status::IOError("LZO Failed to compress.");
    }
    DCHECK(output_offset + (int64_t)written_buffer_len <= output_len);

    // Write the compressed chunk size into the 4 bytes that we reserved.
    writeInt32(written_buffer_len, output, output_offset);

    // Update the offsets.
    output_offset += written_buffer_len;
    input_offset += input_buffer_len;

    return Status::OK();
  }

  int64_t getCompressionOverhead(int64_t buffer_len) {
    // lzo/doc/LZO.FAQ says that the max overhead for LZO1X is
    // (input_block_size / 16) + 64 + 3
    return (buffer_len >> 4) + 64 + 3;
  }
};

}  // namespace

namespace internal {

std::unique_ptr<Codec> MakeLzoCodec() {
  return std::unique_ptr<Codec>(new LzoCodec());
}

}  // namespace internal

}  // namespace util
}  // namespace arrow
