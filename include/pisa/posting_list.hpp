#pragma once

#include "codec/block_codecs.hpp"
#include "multicompression/stats.hpp"
#include "util/block_profiler.hpp"
#include "util/util.hpp"

namespace pisa {
/**
 * Codec types where each codec number (starting from zero) matches
 * with a 'decode function' inside a selector (array) of function
 * pointers. This implementation avoids branches when decoding.
 */
enum CodecTypes {
    block_simdbp,
    block_varintg8iu,
    block_varintgb,
    block_maskedvbyte,
    block_simple8b,
    block_simple16,
    block_streamvbyte,
    block_qmx,
    block_optpfor,
    block_many_ones,
    block_interpolative,
    block_all_ones,

    // Codecs below are used as fallback in order to read posting lists
    // having only a single element. Note that no overhead is required
    // when a fallback codec is used.

    // In the original PISA behavior, when the docs block is 1, 'BIC' is
    // used as 'dummy codec'. Internally nothing is saved, returning the
    // 'sum of values' (overhead) parameter when decoding. In order to allow
    // a faster access time, this behavior was moved to a specific function.
    single_dummy,

    // In addition to the docs block behavior, when the freqs block is also 1,
    // 'TightVariableByte' is used as a BIC's 'fallback codec'. This behavior
    // was moved to a specific function too.
    single_vbyte,
};

typedef uint8_t const *(*decoder)(uint8_t const *, uint32_t *, uint32_t, size_t);
static decoder decoders[] {
    simdbp_block::decode,
    varint_G8IU_block::decode,
    varintgb_block::decode,
    maskedvbyte_block::decode,
    simple8b_block::decode,
    simple16_block::decode,
    streamvbyte_block::decode,
    qmx_block::decode,
    optpfor_block::decode,
    many_ones_block::decode,
    interpolative_block::decode,
    all_ones_block::decode,
    // Note that 'dummy parameters' are used (i.e. 'size_t') to allow generic
    // calls when decoding, such as: decoders[11].decode(in, out, s, n)
    [](uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t) {
        out[0] = sum_of_values;
        return in;
    },
    [](uint8_t const *in, uint32_t *out, uint32_t, size_t) {
        return TightVariableByte::next(in, out[0]);
    },
};

template <bool Profile = false>
struct posting_list {
    static const uint64_t block_size = 128;
    template <typename DocsIterator, typename FreqsIterator>
    // Returns <used doc codecs, used freq codecs>
    auto static write(std::vector<uint8_t> &out,
                      uint32_t n,
                      DocsIterator docs_begin,
                      FreqsIterator freqs_begin)
        -> std::pair<std::vector<pisa::ChunkStatistic>, std::vector<pisa::ChunkStatistic>>
    {
        TightVariableByte::encode_single(n, out);

        // uint64_t block_size = BlockCodec::block_size;
        uint64_t blocks = ceil_div(n, block_size);
        size_t begin_block_maxs = out.size();
        size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
        size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
        out.resize(begin_blocks);

        DocsIterator docs_it(docs_begin);
        FreqsIterator freqs_it(freqs_begin);
        std::vector<uint32_t> docs_buf(block_size);
        std::vector<uint32_t> freqs_buf(block_size);
        uint32_t last_doc(-1);
        uint32_t block_base = 0;

        std::vector<pisa::ChunkStatistic> dcstats;
        std::vector<pisa::ChunkStatistic> fcstats;

        // Foreach block...
        for (size_t b = 0; b < blocks; ++b) {
            uint32_t cur_block_size = ((b + 1) * block_size <= n) ? block_size : (n % block_size);

            // Foreach cell of b-th block...
            for (size_t i = 0; i < cur_block_size; ++i) {
                uint32_t doc(*docs_it++);
                docs_buf[i] = doc - last_doc - 1;
                last_doc = doc;

                freqs_buf[i] = *freqs_it++ - 1;
            }
            *((uint32_t *)&out[begin_block_maxs + 4 * b]) = last_doc;

            // Partition size
             TightVariableByte::encode_single(cur_block_size, out);

            // Reserves space for if n > 1.
            size_t codecs_index = 0;
            if (cur_block_size > 1) {
                codecs_index = out.size();
                out.push_back(0);
            }

            auto doc_codec = encode(
                docs_buf.data(), last_doc - block_base - (cur_block_size - 1), cur_block_size, out);
            auto freq_codec = encode(freqs_buf.data(), std::numeric_limits<uint32_t>::max(), cur_block_size, out);

            pisa::ChunkStatistic dcs(docs_buf, cur_block_size, doc_codec.first, doc_codec.second, false);
            pisa::ChunkStatistic fcs(freqs_buf, cur_block_size, freq_codec.first, freq_codec.second, true);

            // Saves codecs.
            if (cur_block_size > 1) {
                out[codecs_index] += doc_codec.first + (freq_codec.first << 4);
            }

            dcstats.push_back(dcs);
            fcstats.push_back(fcs);

            if (b != blocks - 1) {
                *((uint32_t *)&out[begin_block_endpoints + 4 * b]) = out.size() - begin_blocks;
            }
            block_base = last_doc + 1;
        }
        return {dcstats, fcstats};
    }

    template <typename BlockDataRange>
    static void write_blocks(std::vector<uint8_t> &out,
                             uint32_t n,
                             BlockDataRange const &input_blocks)
    {
        TightVariableByte::encode_single(n, out);
        assert(input_blocks.front().index == 0); // first block must remain first

        uint64_t blocks = input_blocks.size();
        size_t begin_block_maxs = out.size();
        size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
        size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
        out.resize(begin_blocks);

        for (auto const &block : input_blocks) {
            size_t b = block.index;
            // write endpoint
            if (b != 0) {
                *((uint32_t *)&out[begin_block_endpoints + 4 * (b - 1)]) =
                    out.size() - begin_blocks;
            }

            // write max
            *((uint32_t *)&out[begin_block_maxs + 4 * b]) = block.max;

            // copy block
            block.append_docs_block(out);
            block.append_freqs_block(out);
        }
    }

    static auto encode(uint32_t const *in,
                       uint32_t sum_of_values,
                       size_t n,
                       std::vector<uint8_t> &out) -> std::pair<uint8_t, size_t>
    {
        bool docs_encoding = sum_of_values != std::numeric_limits<uint32_t>::max();
        
        // If 'n' is equal to 1...
        if (n == 1) {
            // If this one integer is a doc...
            if (docs_encoding) {
                return {single_dummy, 0};
            } else {
                std::vector<uint8_t> buf;
                TightVariableByte::encode_single(in[0], buf);
                // maskedvbyte_block::encode(in, sum_of_values, n, buf);
                out.insert(out.end(), buf.data(), buf.data() + buf.size());
                return {single_vbyte, buf.size()};
            }
        } else if (all_ones_block::is_encodable(in, sum_of_values, n)) {
            return {block_all_ones, 0};
        }

        // Encodeds of 'in'.
        std::vector<std::vector<uint8_t>> encoded(11);

        // Starts encodes sizes in the max possible value.
        std::vector<size_t> sizes(11, SIZE_MAX);

        if(many_ones_block::encode(in, sum_of_values, n, encoded[block_many_ones])) {
           sizes[block_many_ones] = encoded[block_many_ones].size(); 
        }

        // Encodes according the minimum integers required for varint.
        if (n >= 8) {
            varint_G8IU_block::encode(in, sum_of_values, n, encoded[block_varintg8iu]);
            sizes[block_varintg8iu] = encoded[block_varintg8iu].size();
        }

        // Encodes considering that BP and PFD only handle chunks of block size.
        if (n == block_size) {
            simdbp_block::encode(in, sum_of_values, n, encoded[block_simdbp]);
            optpfor_block::encode(in, sum_of_values, n, encoded[block_optpfor]);
            sizes[block_simdbp] = encoded[block_simdbp].size();
            sizes[block_optpfor] = encoded[block_optpfor].size();
        }

        // Encoders that don't need a special number of integers.
        interpolative_block::encode(in, sum_of_values, n, encoded[block_interpolative]);
        streamvbyte_block::encode(in, sum_of_values, n, encoded[block_streamvbyte]);
        maskedvbyte_block::encode(in, sum_of_values, n, encoded[block_maskedvbyte]);
        simple8b_block::encode(in, sum_of_values, n, encoded[block_simple8b]);
        simple16_block::encode(in, sum_of_values, n, encoded[block_simple16]);
        varintgb_block::encode(in, sum_of_values, n, encoded[block_varintgb]);
        qmx_block::encode(in, sum_of_values, n, encoded[block_qmx]);
        sizes[block_interpolative] = encoded[block_interpolative].size();
        sizes[block_streamvbyte] = encoded[block_streamvbyte].size();
        sizes[block_maskedvbyte] = encoded[block_maskedvbyte].size();
        sizes[block_simple8b] = encoded[block_simple8b].size();
        sizes[block_simple16] = encoded[block_simple16].size();
        sizes[block_varintgb] = encoded[block_varintgb].size();
        sizes[block_qmx] = encoded[block_qmx].size();

        // Selects the encoder that generates the minimum number of bytes.
        uint8_t codec = std::min_element(sizes.begin(), sizes.end()) - sizes.begin();
        size_t out_len = encoded[codec].size();
        out.insert(out.end(), encoded[codec].data(), encoded[codec].data() + out_len);
        return {codec, sizes[codec]};
    }

    class document_enumerator {
       public:
        document_enumerator(uint8_t const *data,
                            uint64_t universe,
                            size_t term_id = 0)
            : m_n(0) // just to silence warnings
              ,
              m_base(TightVariableByte::decode(data, &m_n, 1)),
              m_blocks(ceil_div(m_n, block_size)),
              m_block_maxs(m_base),
              m_block_endpoints(m_block_maxs + 4 * m_blocks),
              m_blocks_data(m_block_endpoints + 4 * (m_blocks - 1)),
              m_universe(universe)
        {
            if (Profile) {
                // std::cout << "OPEN\t" << m_term_id << "\t" << m_blocks << "\n";
                m_block_profile = block_profiler::open_list(term_id, m_blocks);
            }
            m_docs_buf.resize(block_size);
            m_freqs_buf.resize(block_size);
            reset();
        }

        void reset() { decode_docs_block(0); }

        void PISA_ALWAYSINLINE next()
        {
            ++m_pos_in_block;
            if (PISA_UNLIKELY(m_pos_in_block == m_cur_block_size)) {
                if (m_cur_block + 1 == m_blocks) {
                    m_cur_docid = m_universe;
                    return;
                }
                decode_docs_block(m_cur_block + 1);
            } else {
                m_cur_docid += m_docs_buf[m_pos_in_block] + 1;
            }
        }

        void PISA_ALWAYSINLINE next_geq(uint64_t lower_bound)
        {
            assert(lower_bound >= m_cur_docid || position() == 0);
            if (PISA_UNLIKELY(lower_bound > m_cur_block_max)) {
                // binary search seems to perform worse here
                if (lower_bound > block_max(m_blocks - 1)) {
                    m_cur_docid = m_universe;
                    return;
                }

                uint64_t block = m_cur_block + 1;
                while (block_max(block) < lower_bound) {
                    ++block;
                }

                decode_docs_block(block);
            }

            while (docid() < lower_bound) {
                m_cur_docid += m_docs_buf[++m_pos_in_block] + 1;
                assert(m_pos_in_block < m_cur_block_size);
            }
        }

        void PISA_ALWAYSINLINE move(uint64_t pos)
        {
            assert(pos >= position());
            uint64_t block = pos / block_size;
            if (PISA_UNLIKELY(block != m_cur_block)) {
                decode_docs_block(block);
            }
            while (position() < pos) {
                m_cur_docid += m_docs_buf[++m_pos_in_block] + 1;
            }
        }

        uint64_t docid() const { return m_cur_docid; }

        uint64_t PISA_ALWAYSINLINE freq()
        {
            if (!m_freqs_decoded) {
                decode_freqs_block();
            }
            return m_freqs_buf[m_pos_in_block] + 1;
        }

        uint64_t position() const { return m_cur_block * block_size + m_pos_in_block; }

        uint64_t size() const { return m_n; }

        uint64_t num_blocks() const { return m_blocks; }

        uint64_t stats_freqs_size()
        {
            // XXX rewrite in terms of get_blocks()
            uint64_t bytes = 0;
            uint8_t const *ptr = m_blocks_data;
            // static const uint64_t block_size = block_size;
            std::vector<uint32_t> buf(block_size);
            for (size_t b = 0; b < m_blocks; ++b) {
                uint32_t cur_block_size;
                ptr = TightVariableByte::next(ptr, cur_block_size);

                uint32_t cur_base = (b ? block_max(b - 1) : std::numeric_limits<uint32_t>::max()) + 1;
                unpack_codecs(cur_block_size, ptr);
                uint8_t const *freq_ptr =
                    decoders[cur_doc_codec](ptr,
                                            buf.data(),
                                            block_max(b) - cur_base - (cur_block_size - 1),
                                            cur_block_size);
                ptr = decoders[cur_freq_codec](freq_ptr, buf.data(), std::numeric_limits<uint32_t>::max(), cur_block_size);
                bytes += ptr - freq_ptr;
            }

            return bytes;
        }

        struct block_data {
            uint32_t index;
            uint32_t max;
            uint32_t size;
            uint32_t doc_gaps_universe;

            void append_docs_block(std::vector<uint8_t> &out) const
            {
                out.insert(out.end(), docs_begin, freqs_begin);
            }

            void append_freqs_block(std::vector<uint8_t> &out) const
            {
                out.insert(out.end(), freqs_begin, end);
            }

            void decode_doc_gaps(std::vector<uint32_t> &out) const
            {
                out.resize(size);
                decoders[cur_doc_codec](docs_begin, out.data(), doc_gaps_universe, size);
            }

            void decode_freqs(std::vector<uint32_t> &out) const
            {
                out.resize(size);
                decoders[cur_freq_codec](freqs_begin, out.data(), std::numeric_limits<uint32_t>::max(), size);
            }

           private:
            friend class document_enumerator;

            uint8_t const *docs_begin;
            uint8_t const *freqs_begin;
            uint8_t const *end;
        };

       private:
        void unpack_codecs(size_t n, uint8_t const *&block_data)
        {
            bool block_is_greater_than_one = n > 1;
            auto codecs = *block_data;
            // Reads the codecs only if the current block is greater than 1.
            cur_doc_codec = block_is_greater_than_one ? codecs & 15 : single_dummy;
            cur_freq_codec = block_is_greater_than_one ? codecs >> 4 : single_vbyte;
            block_data += block_is_greater_than_one;
        }

        uint32_t block_max(uint32_t block) const { return ((uint32_t const *)m_block_maxs)[block]; }

        void PISA_NOINLINE decode_docs_block(uint64_t block)
        {
            // static const uint64_t block_size = block_size;
            uint32_t endpoint = block ? ((uint32_t const *)m_block_endpoints)[block - 1] : 0;
            uint8_t const *block_data = m_blocks_data + endpoint;
            block_data = TightVariableByte::next(block_data, m_cur_block_size);
            uint32_t cur_base = (block ? block_max(block - 1) : std::numeric_limits<uint32_t>::max()) + 1;
            m_cur_block_max = block_max(block);
            unpack_codecs(m_cur_block_size, block_data);
            m_freqs_block_data =
                decoders[cur_doc_codec](block_data,
                                        m_docs_buf.data(),
                                        m_cur_block_max - cur_base - (m_cur_block_size - 1),
                                        m_cur_block_size);
            intrinsics::prefetch(m_freqs_block_data);

            m_docs_buf[0] += cur_base;

            m_cur_block = block;
            m_pos_in_block = 0;
            m_cur_docid = m_docs_buf[0];
            m_freqs_decoded = false;
            if (Profile) {
                ++m_block_profile[2 * m_cur_block];
            }
        }

        void PISA_NOINLINE decode_freqs_block()
        {
            uint8_t const *next_block = decoders[cur_freq_codec](
                m_freqs_block_data, m_freqs_buf.data(), std::numeric_limits<uint32_t>::max(), m_cur_block_size);
            intrinsics::prefetch(next_block);
            m_freqs_decoded = true;

            if (Profile) {
                ++m_block_profile[2 * m_cur_block + 1];
            }
        }

        uint32_t m_n;
        uint8_t const *m_base;
        uint32_t m_blocks;
        uint8_t const *m_block_maxs;
        uint8_t const *m_block_endpoints;
        uint8_t const *m_blocks_data;
        uint64_t m_universe;

        uint32_t m_cur_block;
        uint32_t m_pos_in_block;
        uint32_t m_cur_block_max;
        uint32_t m_cur_block_size;
        uint32_t m_cur_docid;
        uint8_t cur_doc_codec;
        uint8_t cur_freq_codec;

        uint8_t const *m_freqs_block_data;
        bool m_freqs_decoded;

        std::vector<uint32_t> m_docs_buf;
        std::vector<uint32_t> m_freqs_buf;

        block_profiler::counter_type *m_block_profile;
    };
};
} // namespace pisa