#pragma once

#include "FastPFor/headers/optpfor.h"
#include "FastPFor/headers/variablebyte.h"

#include "VarIntG8IU.h"
#include "interpolative_coding.hpp"
#include "simple16.hpp"
#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"
#include "util/util.hpp"

namespace pisa {

    // workaround: VariableByte::decodeArray needs the buffer size, while we
    // only know the number of values. It also pads to 32 bits. We need to
    // rewrite
    class TightVariableByte {
    public:
        template<uint32_t i>
        static uint8_t extract7bits(const uint32_t val) {
            return static_cast<uint8_t>((val >> (7 * i)) & ((1U << 7) - 1));
        }

        template<uint32_t i>
        static uint8_t extract7bitsmaskless(const uint32_t val) {
            return static_cast<uint8_t>((val >> (7 * i)));
        }

        static void encode(const uint32_t *in, const size_t length,
                           uint8_t *out, size_t& nvalue)
        {
            uint8_t * bout = out;
            for (size_t k = 0; k < length; ++k) {
                const uint32_t val(in[k]);
                /**
                 * Code below could be shorter. Whether it could be faster
                 * depends on your compiler and machine.
                 */
                if (val < (1U << 7)) {
                    *bout = static_cast<uint8_t>(val | (1U << 7));
                    ++bout;
                } else if (val < (1U << 14)) {
                    *bout = extract7bits<0> (val);
                    ++bout;
                    *bout = extract7bitsmaskless<1> (val) | (1U << 7);
                    ++bout;
                } else if (val < (1U << 21)) {
                    *bout = extract7bits<0> (val);
                    ++bout;
                    *bout = extract7bits<1> (val);
                    ++bout;
                    *bout = extract7bitsmaskless<2> (val) | (1U << 7);
                    ++bout;
                } else if (val < (1U << 28)) {
                    *bout = extract7bits<0> (val);
                    ++bout;
                    *bout = extract7bits<1> (val);
                    ++bout;
                    *bout = extract7bits<2> (val);
                    ++bout;
                    *bout = extract7bitsmaskless<3> (val) | (1U << 7);
                    ++bout;
                } else {
                    *bout = extract7bits<0> (val);
                    ++bout;
                    *bout = extract7bits<1> (val);
                    ++bout;
                    *bout = extract7bits<2> (val);
                    ++bout;
                    *bout = extract7bits<3> (val);
                    ++bout;
                    *bout = extract7bitsmaskless<4> (val) | (1U << 7);
                    ++bout;
                }
            }
            nvalue = bout - out;
        }

        static void encode_single(uint32_t val, std::vector<uint8_t>& out)
        {
            uint8_t buf[5];
            size_t nvalue;
            encode(&val, 1, buf, nvalue);
            out.insert(out.end(), buf, buf + nvalue);
        }

        static uint8_t const* decode(const uint8_t *in, uint32_t *out, size_t n)
        {
            const uint8_t * inbyte = in;
            for (size_t i = 0; i < n; ++i) {
                unsigned int shift = 0;
                for (uint32_t v = 0; ; shift += 7) {
                    uint8_t c = *inbyte++;
                    v += ((c & 127) << shift);
                    if ((c & 128)) {
                        *out++ = v;
                        break;
                    }
                }
            }
            return inbyte;
        }

        static void decode(const uint8_t *in, uint32_t *out, size_t len, size_t &n)
        {
            const uint8_t *inbyte = in;
            while (inbyte < in + len) {
                unsigned int shift = 0;
                for (uint32_t v = 0;; shift += 7) {
                    uint8_t c = *inbyte++;
                    v += ((c & 127) << shift);
                    if ((c & 128)) {
                        *out++ = v;
                        n += 1;
                        break;
                    }
                }
            }
        }

        /**
         * Decodes next integer. This method is optimal when an unique integer
         * must be decoded (note: SIMD isn't used here).
        */
        static uint8_t const *next(uint8_t const *in, uint32_t &val) {
            // Gets the first byte avoiding the continuation bit.
            uint32_t c = *in++;
            val = c & 0x7F;
            // Iterates while the continuation bit be true.
            for (uint32_t shift = 7; !(c & 0x80); shift += 7) {
                // Adds, to the output integer, the byte with the adequate shifting.
                val |= (((c = *in++) & 0x7F) << shift);
            }
            return in;
        }
    };

    struct interpolative_block {
        static const uint64_t block_size = 128;

        static void encode(uint32_t const* in, uint32_t sum_of_values,
                           size_t n, std::vector<uint8_t>& out)
        {
            assert(n <= block_size);
            thread_local std::vector<uint32_t> inbuf(block_size);
            thread_local std::vector<uint32_t> outbuf;
            inbuf[0] = *in;
            for (size_t i = 1; i < n; ++i) {
                inbuf[i] = inbuf[i - 1] + in[i];
            }

            if (sum_of_values == std::numeric_limits<uint32_t>::max()) {
                sum_of_values = inbuf[n - 1];
                TightVariableByte::encode_single(sum_of_values, out);
            }

            bit_writer bw(outbuf);
            bw.write_interpolative(inbuf.data(), n - 1, 0, sum_of_values);
            uint8_t const* bufptr = (uint8_t const*)outbuf.data();
            out.insert(out.end(), bufptr,
                       bufptr + ceil_div(bw.size(), 8));
        }

        static uint8_t const* PISA_NOINLINE decode(uint8_t const* in, uint32_t* out,
                                                 uint32_t sum_of_values, size_t n)
        {
            assert(n <= block_size);
            if (sum_of_values == std::numeric_limits<uint32_t>::max()) {
                in = TightVariableByte::next(in, sum_of_values);
            }
            // Assigns last array value and read remaining.
            out[--n] = sum_of_values;
            bit_reader br((uint32_t const*)in);
            br.read_interpolative(out, n, 0, sum_of_values);

            // Computes dgaps of the decoded array.
            for (; n > 0; --n) {
                out[n] -= out[n - 1];
            }
            
            // Computes the number of read bytes.
            size_t readed = ceil_div(br.position(), 8);
            return in + readed;
        }
    };

    struct optpfor_block {

        struct codec_type : FastPForLib::OPTPFor<4, FastPForLib::Simple16<false>> {

            uint8_t const* force_b;

            uint32_t findBestB(const uint32_t *in, uint32_t len) {
                // trick to force the choice of b from a parameter
                if (force_b) {
                    return *force_b;
                }

                // this is mostly a cut&paste from FastPFor, but we stop the
                // optimization early as the b to test becomes larger than maxb
                uint32_t b = 0;
                uint32_t bsize = std::numeric_limits<uint32_t>::max();
                const uint32_t mb = FastPForLib::maxbits(in,in+len);
                uint32_t i = 0;
                while(mb > 28 + possLogs[i]) ++i; // some schemes such as Simple16 don't code numbers greater than 28

                for (; i < possLogs.size(); i++) {
                    if (possLogs[i] > mb && possLogs[i] >= mb) break;
                    const uint32_t csize = tryB(possLogs[i], in, len);

                    if (csize <= bsize) {
                        b = possLogs[i];
                        bsize = csize;
                    }
                }
                return b;
            }

        };

        static const uint64_t block_size = codec_type::BlockSize;

        static void encode(uint32_t const* in, uint32_t sum_of_values,
                           size_t n, std::vector<uint8_t>& out,
                           uint8_t const* b=nullptr) // if non-null forces b
        {
            thread_local codec_type optpfor_codec;
            thread_local std::vector<uint8_t> buf(2 * 4 * block_size);
            assert(n <= block_size);
            size_t out_len = buf.size();

            optpfor_codec.force_b = b;
            optpfor_codec.encodeBlock(in, reinterpret_cast<uint32_t*>(buf.data()),
                                      out_len);
            out_len *= 4;
            out.insert(out.end(), buf.data(), buf.data() + out_len);
        }

        static uint8_t const* PISA_NOINLINE decode(uint8_t const* in, uint32_t* out,
                                                 uint32_t sum_of_values, size_t n)
        {
            thread_local codec_type optpfor_codec; // pfor decoding is *not* thread-safe
            assert(n <= block_size);
            size_t out_len = block_size;
            uint8_t const* ret;

            ret = reinterpret_cast<uint8_t const*>
                (optpfor_codec.decodeBlock(reinterpret_cast<uint32_t const*>(in),
                                           out, out_len));
            assert(out_len == n);
            return ret;
        }
    };

    struct varint_G8IU_block {
        static const uint64_t block_size = 128;

        struct codec_type : VarIntG8IU {

            // rewritten version of decodeBlock optimized for when the output
            // size is known rather than the input
            // the buffers pointed by src and dst must be respectively at least
            // 9 and 8 elements large
            uint32_t decodeBlock(uint8_t const*& src, uint32_t* dst) const
            {
                uint8_t desc = *src;
                src += 1;
                const __m128i data = _mm_lddqu_si128 (reinterpret_cast<__m128i const*>(src));
                src += 8;
                const __m128i result = _mm_shuffle_epi8 (data, vecmask[desc][0]);
                _mm_storeu_si128(reinterpret_cast<__m128i*> (dst), result);
                int readSize = maskOutputSize[desc];

                if ( readSize > 4 ) {
                    const __m128i result2 = _mm_shuffle_epi8 (data, vecmask[desc][1]);//__builtin_ia32_pshufb128(data, shf2);
                    _mm_storeu_si128(reinterpret_cast<__m128i *> (dst + 4), result2);//__builtin_ia32_storedqu(dst + (16), result2);
                }

                return readSize;
            }
        };

        static void encode(uint32_t const* in, uint32_t sum_of_values,
                           size_t n, std::vector<uint8_t>& out)
        {
            thread_local codec_type varint_codec;
            thread_local std::vector<uint8_t> buf(2 * 4 * block_size);
            assert(n <= block_size);
            size_t out_len = buf.size();

            const uint32_t * src = in;
            unsigned char* dst = buf.data();
            size_t srclen = n * 4;
            size_t dstlen = out_len;
            out_len = 0;
            while (srclen > 0 && dstlen >= 9) {
                out_len += varint_codec.encodeBlock(src, srclen, dst, dstlen);
            }
            assert(srclen == 0);
            out.insert(out.end(), buf.data(), buf.data() + out_len);
        }

        // we only allow varint to be inlined (others have PISA_NOILINE)
        static uint8_t const* decode(uint8_t const* in, uint32_t* out,
                                     uint32_t sum_of_values, size_t n)
        {
            static codec_type varint_codec; // decodeBlock is thread-safe
            assert(n <= block_size);
            size_t out_len = 0;
            uint8_t const* src = in;
            uint32_t* dst = out;
            while (out_len <= (n - 8)) {
                out_len += varint_codec.decodeBlock(src, dst + out_len);
            }

            // decodeBlock can overshoot, so we decode the last blocks in a
            // local buffer
            while (out_len < n) {
                uint32_t buf[8];
                size_t read = varint_codec.decodeBlock(src, buf);
                size_t needed = std::min(read, n - out_len);
                memcpy(dst + out_len, buf, needed * 4);
                out_len += needed;
            }
            assert(out_len == n);
            return src;
        }
    };

    /**
     * Allows to encode blocks consisting in only 1s. However, given that
     * to each dgap in a block is subtracted a '1' (except for the first
     * value of the docs list) these 1s actually will be decoded as 0s.
     * Furthermore, the first value of the documents list can be any,
     * since it can be recovered based on the 'sum of values' parameter.
     */
    struct all_ones_block {
        static const uint64_t block_size = 128;

        static bool is_encodable(uint32_t const* in, uint32_t sum_of_values, size_t n) {
            bool encoding_freqs = sum_of_values == std::numeric_limits<uint32_t>::max();

            // Checks first value if there are compressing freqs.
            if (encoding_freqs && *in != 0) {
                return false;
            }

            // Iterates over 'in', while as long as 'current value' is 0.
            for (auto i = 1; i < n; i++) {
                if (*++in > 0) {
                    return false;
                }
            }
            return true;
        }

        static uint8_t const* decode(uint8_t const* in, uint32_t* out,
                                     uint32_t sum_of_values, size_t n)
        {
            // Computes the first value in case of decoding documents. Note:
            // 'sum_of_values' already has substracted 'n - 1', so that its
            // value is assigned directly.
            bool decoding_docs = sum_of_values != std::numeric_limits<uint32_t>::max();
            *out++ = decoding_docs * sum_of_values;

            // Sets remaining integers as 0. Why uses memset instead std::fill?
            // See: https://lemire.me/blog/2020/01/20/filling-large-arrays-with-zeroes-quickly-in-c/
            memset(out, 0, (n - 1) * 4);
            return in;
        }
    };

    /**
     * Allows to encode a block consisting in at least a 25% of 1s with
     * 'all ones'; and saving the exceptions and its positions (actually
     * its gaps) by using Simple16.
     */
    struct many_ones_block {
        static const uint64_t block_size = 128;

        static uint32_t compute_exceptions(uint32_t const* in, uint32_t sum_of_values,
                                           size_t n, std::vector<uint32_t>& out) {
            // If there are encoding docs, 'curr_value_pos' must be start from 1.
            uint32_t curr_value_pos = sum_of_values != std::numeric_limits<uint32_t>::max();

            thread_local uint32_t exceptions[block_size * 2];        // Buffer of positions + exceptions (gaps).
            uint32_t exception_count = 0;                            // Exception count (returned value).
            int32_t last_exception_pos = curr_value_pos ? 0 : -1;    // Used to compute position gaps.

            // Computes exceptions.
            for (; curr_value_pos < n; curr_value_pos++) {
                // Checks if the current value is an exception.
                uint32_t value = in[curr_value_pos];
                if (value == 0)
                    continue;
                
                // Saves exception position gap, no subtracting the value 1 to
                // the first index because, when encoding freqs, it can be 0.
                uint32_t gap = curr_value_pos - last_exception_pos - 1;
                exceptions[exception_count] = gap;

                // Saves exception.
                exceptions[block_size + exception_count] = value - 1;
                last_exception_pos = curr_value_pos;
                exception_count++;
            }
            out.insert(out.end(), exceptions, exceptions + exception_count);
            out.insert(out.end(), exceptions + block_size, exceptions + block_size + exception_count);
            return exception_count;
        }

        static bool encode(uint32_t const* in, uint32_t sum_of_values,
                           size_t n, std::vector<uint8_t>& out)
        {
            std::vector<uint32_t> exceptions;
            uint32_t exception_count = compute_exceptions(in, sum_of_values, n, exceptions);

            // If the exceptions covers 75% of the list, there is not much
            // sense to encoding using 'many ones'.
            if (exception_count > n * 0.75) {
                return false;
            }
            out.push_back(exception_count - 1);
            simple16_block::encode(exceptions.data(), sum_of_values, exception_count * 2, out);
            return true;
        }

        static uint8_t const* decode(uint8_t const* in, uint32_t* out,
                                     uint32_t sum_of_values, size_t n)
        {
            all_ones_block::decode(in, out, sum_of_values, n);

            // Decodes exceptions.
            uint32_t exception_count = *in++ + 1;
            uint32_t to_decode = exception_count * 2;
            uint32_t exceptions[to_decode];
            in = simple16_block::decode(in, exceptions, sum_of_values, to_decode);

            // Computes the first exception.
            bool decoding_docs = sum_of_values != std::numeric_limits<uint32_t>::max();
            uint32_t exception_pos = exceptions[0] + decoding_docs;
            uint32_t exception_value_pos = exception_count;
            out[exception_pos] += exceptions[exception_value_pos] + 1;

            // Computes remaining exceptions.
            uint32_t sum_of_exceptions = out[exception_pos];
            for(auto i = 1; i < exception_count; i++) {
                exception_pos += exceptions[i] + 1;
                uint32_t exception_value = exceptions[++exception_value_pos] + 1;
                out[exception_pos] += exception_value;
                sum_of_exceptions += exception_value;
            }
            // If there are decoding docs, rebuild 1st number subtracting 'sum_of_exceptions'.
            out[0] -= decoding_docs * sum_of_exceptions;
            return in;
        }
    };
}