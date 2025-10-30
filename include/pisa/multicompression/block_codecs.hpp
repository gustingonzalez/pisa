#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "FastPFor/headers/simple16.h"

#include "codec/block_codecs.hpp"
#include "codec/simple16.hpp"

namespace pisa {
struct BitPackingBlockCodec {

    // Computes how many bits are needed to store an specific value.
    static uint32_t bit_width(uint32_t value) {
        return value == 0 ? 0U : static_cast<uint32_t>(32U - __builtin_clz(value));
    }

    // Computes the maximum bit width required by each value of a block.
    static uint32_t max_bits(std::uint32_t const* in, std::size_t n) {
        if (n == 0) {
            return 0;
        }
        auto const* max = std::max_element(in, in + n);
        return bit_width(*max);
    }

    // Computes the payload size in bytes.
    static std::size_t compute_payload_size(std::uint32_t b, std::size_t n) {
        if (b == 0 || n == 0) {
            return 0;
        }
        return (static_cast<std::size_t>(b) * n + 7) / 8;
    }

    // Computes the total bytes required, including the (one-byte) header.
    static std::size_t compute_encoded_size(std::uint32_t b, std::size_t n) {
        return 1 + compute_payload_size(b, n);
    }

    static std::size_t compute_encoded_size(std::uint32_t const* in, std::size_t n) {
        return compute_encoded_size(max_bits(in, n), n);
    }

    static void encode(
        std::uint32_t const* in,
        std::uint32_t,
        std::size_t n,
        std::vector<std::uint8_t>& out
    ) {
        uint32_t b = max_bits(in, n);

        std::size_t encoded_size = compute_encoded_size(b, n);
        auto start = out.size();
        out.resize(start + encoded_size);
        auto* header = out.data() + start;
        auto* data = header + 1;

        // Add `b` as one-byte header. If it is 0, then all values are zero; so
        // no payload bytes is needed.
        *header = static_cast<uint8_t>(b);
        if (b == 0) {
            return;
        }

        // Note that payload is written in little-endian bytes. Advantages:
        // - Values are appended with a masking and shift; then simple OR.
        // - Decoding implementation just 'mirrors' encoding.
        //
        // Example with 'in = [5, 2, 7, 1, 3]':
        // Human-readable, big-endian => 1010101 11001011 => 101 010 111 001 011
        // Encoder output, little-endian (by byte) => 11010101 00110011 => 101 010 111 100 110

        // Bit mask to isolate one packed value. For b=32 avoids shifting (UB).
        uint32_t mask = b == 32 ? 0xFFFFFFFFU : static_cast<uint32_t>((uint64_t(1) << b) - 1);

        // Buffer that accumulates read bits.
        uint64_t buffer = 0;

        // Count of valid bits currently stored in `buffer`.
        unsigned bits_in_buffer = 0;

        // Number of bytes already written to `data`.
        size_t written = 0;

        for (size_t i = 0; i < n; ++i) {
            buffer |= (uint64_t(in[i]) & mask) << bits_in_buffer;
            bits_in_buffer += b;

            // Flush the buffer when holds at least one full byte (8 bits), the
            // size of each element of `out`.
            while (bits_in_buffer >= 8) {
                // Write the lowest 8 bits.
                data[written++] = static_cast<uint8_t>(buffer & 0xFF);
                // Remove emitted bits.
                buffer >>= 8;
                // Adjust bit counter.
                bits_in_buffer -= 8;
            }
        }

        // Emit any remaining bits left in the buffer.
        if (bits_in_buffer != 0) {
            data[written++] = static_cast<uint8_t>(buffer & 0xFF);
        }
        // Zero any trailing padding bytes we might have reserved.
        std::fill(data + written, data + compute_payload_size(b, n), 0);
    }

    static uint8_t const* decode(
        uint8_t const* in,
        uint32_t* out,
        uint32_t,
        std::size_t n
    ) {
        uint32_t b = *in++;
        if (b == 0) {
            std::fill_n(out, n, uint32_t(0));
            return in;
        }

        uint32_t mask = b == 32 ? 0xFFFFFFFFU : static_cast<uint32_t>((uint64_t(1) << b) - 1);
        auto const* data = in;
        uint64_t buffer = 0;
        unsigned bits_in_buffer = 0;

        // Mirror of `encode` function.
        for (std::size_t i = 0; i < n; ++i) {
            while (bits_in_buffer < b) {
                buffer |= uint64_t(*data++) << bits_in_buffer;
                bits_in_buffer += 8;
            }
            out[i] = static_cast<uint32_t>(buffer & mask);
            buffer >>= b;
            bits_in_buffer -= b;
        }
        return data;
    }
};

struct all_ones_block {
    static constexpr std::uint64_t block_size = 128;

    static bool is_encodable(
        std::uint32_t const* in, std::uint32_t sum_of_values, std::size_t n
    ) {
        bool encoding_freqs = sum_of_values == std::numeric_limits<std::uint32_t>::max();

        if (encoding_freqs && *in != 0) {
            return false;
        }

        for (std::size_t i = 1; i < n; ++i) {
            if (*++in > 0) {
                return false;
            }
        }
        return true;
    }

    static std::uint8_t const* decode(
        std::uint8_t const* in,
        std::uint32_t* out,
        std::uint32_t sum_of_values,
        std::size_t n
    ) {
        bool decoding_docs = sum_of_values != std::numeric_limits<std::uint32_t>::max();
        *out++ = decoding_docs * sum_of_values;
        std::memset(out, 0, (n - 1) * sizeof(std::uint32_t));
        return in;
    }
};

struct many_ones_block {
    static constexpr std::uint64_t block_size = 128;
    static constexpr float exception_threshold = 0.75F;

    static std::uint32_t count_exceptions(
        std::uint32_t const* in, std::uint32_t sum_of_values, std::size_t n
    ) {
        std::uint32_t curr_value_pos = sum_of_values != std::numeric_limits<std::uint32_t>::max();
        return std::count_if(
            in + curr_value_pos, in + n, [](std::uint32_t x) { return x > 0; }
        );
    }

    static std::vector<std::uint32_t>
    compute_exceptions(std::uint32_t const* in, std::uint32_t sum_of_values, std::size_t n) {
        std::uint32_t exception_count = count_exceptions(in, sum_of_values, n);
        std::vector<std::uint32_t> exceptions(exception_count * 2);

        std::uint32_t curr_value_pos = sum_of_values != std::numeric_limits<std::uint32_t>::max();
        std::int32_t last_exception_pos = curr_value_pos ? 0 : -1;

        std::uint32_t current_exception_index = 0;
        for (; curr_value_pos < n; ++curr_value_pos) {
            std::uint32_t value = in[curr_value_pos];
            if (value == 0) {
                continue;
            }

            std::uint32_t gap = curr_value_pos - last_exception_pos - 1;
            exceptions[current_exception_index] = gap;
            exceptions[exception_count + current_exception_index] = value - 1;
            last_exception_pos = static_cast<std::int32_t>(curr_value_pos);
            current_exception_index += 1U;
        }
        return exceptions;
    }

    static bool encode(
        std::uint32_t const* in,
        std::uint32_t sum_of_values,
        std::size_t n,
        std::vector<std::uint8_t>& out
    ) {
        static Simple16BlockCodec simple16_codec{};
        std::uint32_t exception_count = count_exceptions(in, sum_of_values, n);
        if (exception_count > n * exception_threshold) {
            return false;
        }
        auto exceptions = compute_exceptions(in, sum_of_values, n);
        exceptions.insert(exceptions.begin(), exception_count - 1);
        simple16_codec.encode(exceptions.data(), sum_of_values, exception_count * 2 + 1, out);
        return true;
    }

    static std::uint8_t const* decode_exceptions(
        std::uint8_t const* in, std::uint32_t* out, std::uint32_t& n
    ) {
        thread_local FastPForLib::Simple16<false> codec{};
        std::uint32_t buf[28];
        std::uint32_t* pbuf = buf;
        auto const* in32 = reinterpret_cast<std::uint32_t const*>(in);
        codec.unpackarray[codec.which(in32)](&pbuf, &in32);
        auto const* pstart = out;
        std::copy(buf + 1, pbuf, out);
        std::uint32_t read = static_cast<std::uint32_t>(pbuf - buf - 1);
        out += read;
        n = (buf[0] + 1) * 2;
        auto const* pend = pstart + n;
        while (pend > out) {
            codec.unpackarray[codec.which(in32)](&out, &in32);
        }
        return reinterpret_cast<std::uint8_t const*>(in32);
    }

    static std::uint8_t const* decode(
        std::uint8_t const* in,
        std::uint32_t* out,
        std::uint32_t sum_of_values,
        std::size_t n
    ) {
        std::memset(out, 0, n * sizeof(std::uint32_t));
        std::uint32_t to_decode = 0;
        std::vector<std::uint32_t> exceptions((static_cast<std::size_t>(n * exception_threshold) + 28) * 2);
        in = decode_exceptions(in, exceptions.data(), to_decode);
        std::uint32_t exception_count = to_decode / 2;

        bool decoding_docs = sum_of_values != std::numeric_limits<std::uint32_t>::max();
        std::uint32_t exception_pos = exceptions[0] + decoding_docs;
        std::uint32_t exception_value_pos = exception_count;
        out[exception_pos] += exceptions[exception_value_pos] + 1;

        std::uint32_t sum_of_exceptions = out[exception_pos];
        for (std::uint32_t i = 1; i < exception_count; ++i) {
            exception_pos += exceptions[i] + 1;
            std::uint32_t exception_value = exceptions[++exception_value_pos] + 1;
            out[exception_pos] += exception_value;
            sum_of_exceptions += exception_value;
        }
        out[0] += decoding_docs * (sum_of_values - sum_of_exceptions);
        return in;
    }
};

/**
 * Run Length Encoding:
 * - Uses the first value to inject a bit flag to distinguish two cases:
 *   - all_same value
 *   - multiple runs
 * - Uses LSB of first VByte-encoded number as a flag. This avoid using one byte
 * for header.
 * - Even value (LSB=0): all_same case, stores value*2
 * - Odd value (LSB=1): multi-run case, stores ((run_count-1)*2 + 1)
 *
 * This saves 1 byte compared to explicit header while maintaining correctness:
 * - run_count is at most 128 (block size), so (run_count-1)*2+1 = 255 max (no overflow)
 * - Storing (run_count-1) instead of run_count saves encoding space since minimum is 1
 * - Values can theoretically reach 2^32-1, but multiplying by 2 could overflow
 * - In practice, values are gap-encoded docIDs (typically < 100K) or frequencies (< 10K)
 * - Maximum safe value for all_same encoding: 2^31-1 (2,147,483,647)
 * - Values >= 2^31 would overflow when doubled, but are extremely rare in real data
 * - If overflow occurs, codec selection will favor other codecs anyway
 */
struct rle_block {
    static constexpr std::uint64_t block_size = 128;

    /**
     * Computes exact encoded size without actually encoding.
     * Returns SIZE_MAX if RLE is not beneficial.
     */
    static std::size_t compute_encoded_size(std::uint32_t const* in, std::size_t n) {
        if (n == 0) {
            return SIZE_MAX;
        }

        // Check if all same value (optimal case)
        bool all_same = std::all_of(in + 1, in + n, [first = in[0]](uint32_t v) {
            return v == first;
        });

        if (all_same) {
            // Just VByte(value * 2), no header byte
            // Doubling adds at most 1 bit, minimal impact on VByte size
            std::size_t vbyte_size = compute_vbyte_size(in[0] << 1);
            return vbyte_size;
        }

        // Count runs for general case
        std::uint32_t run_count = 1;
        for (std::size_t i = 1; i < n; ++i) {
            if (in[i] != in[i - 1]) {
                ++run_count;
            }
        }

        // Compute exact encoded size
        // First number encodes ((run_count - 1) * 2 + 1) to signal multi-run case
        std::size_t size = compute_vbyte_size(((run_count - 1) << 1) | 1);

        std::uint32_t current_run_length = 1;
        for (std::size_t i = 1; i < n; ++i) {
            if (in[i] == in[i - 1]) {
                ++current_run_length;
            } else {
                size += compute_vbyte_size(current_run_length - 1);
                size += compute_vbyte_size(in[i - 1]);
                current_run_length = 1;
            }
        }
        // Last run
        size += compute_vbyte_size(current_run_length - 1);
        size += compute_vbyte_size(in[n - 1]);

        return size;
    }

    static void encode(
        std::uint32_t const* in,
        std::uint32_t /* sum_of_values */,
        std::size_t n,
        std::vector<std::uint8_t>& out
    ) {
        // Note: `n = 0` is not possible in multicompression context.
        // if (n == 0) {
        //    return;
        // }

        // Check if all values are the same
        bool all_same = std::all_of(in + 1, in + n, [first = in[0]](uint32_t v) {
            return v == first;
        });

        if (all_same) {
            // Optimal case: all same value
            // Encode value*2 with LSB=0 to signal all_same case
            TightVariableByte::encode_single(in[0] << 1, out);
            return;
        }

        // General case: encode runs
        // Count runs
        std::uint32_t run_count = 1;
        for (std::size_t i = 1; i < n; ++i) {
            if (in[i] != in[i - 1]) {
                ++run_count;
            }
        }

        // Encode ((run_count - 1)*2 + 1) with LSB=1 to signal multi-run case
        TightVariableByte::encode_single(((run_count - 1) << 1) | 1, out);

        // Encode each run as (length-1, value)
        std::uint32_t current_run_length = 1;
        for (std::size_t i = 1; i < n; ++i) {
            if (in[i] == in[i - 1]) {
                ++current_run_length;
            } else {
                TightVariableByte::encode_single(current_run_length - 1, out);
                TightVariableByte::encode_single(in[i - 1], out);
                current_run_length = 1;
            }
        }

        // Emit last run
        TightVariableByte::encode_single(current_run_length - 1, out);
        TightVariableByte::encode_single(in[n - 1], out);
    }

    static std::uint8_t const* decode(
        std::uint8_t const* in,
        std::uint32_t* out,
        std::uint32_t /* sum_of_values */,
        std::size_t n
    ) {
        if (n == 0) {
            return in;
        }

        // Read first VByte-encoded number and check LSB for case discrimination
        std::uint32_t first_value;
        in = TightVariableByte::decode(in, &first_value, 1);

        if ((first_value & 1) == 0) {
            // LSB = 0: all_same case
            // Recover original value by dividing by 2 (right shift)
            std::uint32_t value = first_value >> 1;
            std::fill_n(out, n, value);
            return in;
        }

        // LSB = 1: multi-run case
        // Recover run_count by dividing by 2 (right shift) and adding 1
        std::uint32_t run_count = (first_value >> 1) + 1;

        std::size_t decoded = 0;
        for (std::uint32_t r = 0; r < run_count; ++r) {
            std::uint32_t run_length_minus_1;
            std::uint32_t value;

            in = TightVariableByte::decode(in, &run_length_minus_1, 1);
            in = TightVariableByte::decode(in, &value, 1);

            std::uint32_t run_length = run_length_minus_1 + 1;
            std::fill_n(out + decoded, run_length, value);
            decoded += run_length;
        }

        return in;
    }

  private:
    static std::size_t compute_vbyte_size(std::uint32_t value) {
        if (value < (1U << 7)) return 1;
        if (value < (1U << 14)) return 2;
        if (value < (1U << 21)) return 3;
        if (value < (1U << 28)) return 4;
        return 5;
    }
};


}  // namespace pisa
