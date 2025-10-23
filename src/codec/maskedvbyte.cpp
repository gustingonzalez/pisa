#include "codec/maskedvbyte.hpp"
#include "MaskedVByte/include/varintdecode.h"
#include "MaskedVByte/include/varintencode.h"
#include "codec/block_codecs.hpp"

namespace pisa {

void MaskedVByteBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);
    auto* src = const_cast<uint32_t*>(in);
    thread_local std::array<std::uint8_t, 2 * m_block_size * sizeof(std::uint32_t)> buf{};
    size_t out_len = vbyte_encode(src, n, buf.data());
    out.insert(out.end(), buf.data(), buf.data() + out_len);
}

uint8_t const*
MaskedVByteBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    assert(n <= m_block_size);
    auto read = masked_vbyte_decode(in, out, n);
    return in + read;
}

}  // namespace pisa
