#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "FastPFor/headers/simple16.h"

#include "codec/simple16.hpp"

namespace pisa {
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

}  // namespace pisa
