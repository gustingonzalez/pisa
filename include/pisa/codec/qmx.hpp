#pragma once

#include "QMX/qmx.hpp"
#include "codec/block_codecs.hpp"

namespace pisa {
struct qmx_block {
    static const uint64_t block_size = 64;
    static const uint64_t overflow = 512;

    static void encode(uint32_t const *in,
                       uint32_t sum_of_values,
                       size_t n,
                       std::vector<uint8_t> &out) {

        assert(n <= block_size);
        uint32_t *src = const_cast<uint32_t *>(in);
        thread_local QMX::compress_integer_qmx_improved qmx_codec;
        thread_local std::vector<uint8_t> buf(2 * n * sizeof(uint32_t) + overflow);

        size_t out_len = qmx_codec.encode(buf.data(), buf.size(), in, n);
        TightVariableByte::encode_single(out_len, out);
        out.insert(out.end(), buf.data(), buf.data() + out_len);
    }
    static uint8_t const *decode(uint8_t const *in,
                                 uint32_t *out,
                                 uint32_t sum_of_values,
                                 size_t n) {
        static QMX::compress_integer_qmx_improved qmx_codec; // decodeBlock is thread-safe
        assert(n <= block_size);
        uint32_t enc_len = 0;
        in = TightVariableByte::decode(in, &enc_len, 1);
        std::vector<uint32_t> buf(2 * n + overflow);
        qmx_codec.decode(buf.data(), n, in, enc_len);
        for (size_t i = 0; i < n; ++i)
        {
            *out = buf[i];
            ++out;
        }
        return in + enc_len;
    }
};
} // namespace pisa
