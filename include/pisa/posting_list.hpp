#pragma once

#include "codec/block_codecs.hpp"
#include "util/util.hpp"
#include "util/block_profiler.hpp"

namespace pisa {

    enum CodecTypes
    {
        block_varintg8iu,
        block_streamvbyte,
        block_maskedvbyte,
        block_interpolative,
        block_qmx,
        block_simple8b,
        block_simple16,
        block_simdbp,
        block_optpfor
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
            -> std::pair<std::vector<CodecTypes>, std::vector<CodecTypes>>
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

            std::vector<CodecTypes> doc_codecs;
            std::vector<CodecTypes> freq_codecs;

            // Foreach block...
            for (size_t b = 0; b < blocks; ++b) {
                uint32_t cur_block_size =
                    ((b + 1) * block_size <= n) ? block_size : (n % block_size);

                // Foreach cell of b-th block...
                for (size_t i = 0; i < cur_block_size; ++i) {
                    uint32_t doc(*docs_it++);
                    docs_buf[i] = doc - last_doc - 1;
                    last_doc = doc;

                    freqs_buf[i] = *freqs_it++ - 1;
                }
                *((uint32_t *)&out[begin_block_maxs + 4 * b]) = last_doc;
                
                // Reserves space for codecs.
                size_t codecs_index = out.size();
                out.push_back(0);
                
                uint8_t doc_codec = encode(docs_buf.data(), last_doc - block_base - (cur_block_size - 1),
                                        cur_block_size, out);
                uint8_t freq_codec = encode(freqs_buf.data(), uint32_t(-1), cur_block_size, out);
                
                // Saves codecs.
                out[codecs_index] += doc_codec + (freq_codec << 4);
                
                doc_codecs.push_back(static_cast<CodecTypes>(doc_codec));
                freq_codecs.push_back(static_cast<CodecTypes>(freq_codec));

                if (b != blocks - 1) {
                    *((uint32_t *)&out[begin_block_endpoints + 4 * b]) = out.size() - begin_blocks;
                }
                block_base = last_doc + 1;
            }

            return {doc_codecs, freq_codecs};
        }

        template <typename BlockDataRange>
        static void write_blocks(std::vector<uint8_t>& out, uint32_t n,
                                 BlockDataRange const& input_blocks)
        {
            TightVariableByte::encode_single(n, out);
            assert(input_blocks.front().index == 0); // first block must remain first

            uint64_t blocks = input_blocks.size();
            size_t begin_block_maxs = out.size();
            size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
            size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
            out.resize(begin_blocks);

            for (auto const& block: input_blocks) {
                size_t b = block.index;
                // write endpoint
                if (b != 0) {
                    *((uint32_t*)&out[begin_block_endpoints + 4 * (b - 1)]) = out.size() - begin_blocks;
                }

                // write max
                *((uint32_t*)&out[begin_block_maxs + 4 * b]) = block.max;

                // copy block
                block.append_docs_block(out);
                block.append_freqs_block(out);
            }
        }

        static uint8_t encode(uint32_t const* in, uint32_t sum_of_values,
                           size_t n, std::vector<uint8_t>& out)
        {
            std::vector<std::vector<uint8_t> > encodes(9);

            varint_G8IU_block::encode(in, sum_of_values, n, encodes[block_varintg8iu]);
            streamvbyte_block::encode(in, sum_of_values, n, encodes[block_streamvbyte]);
            maskedvbyte_block::encode(in, sum_of_values, n, encodes[block_maskedvbyte]);
            interpolative_block::encode(in, sum_of_values, n, encodes[block_interpolative]);
            qmx_block::encode(in, sum_of_values, n, encodes[block_qmx]);
            simple8b_block::encode(in, sum_of_values, n, encodes[block_simple8b]);
            simple16_block::encode(in, sum_of_values, n, encodes[block_simple16]);
            simdbp_block::encode(in, sum_of_values, n, encodes[block_simdbp]);
            optpfor_block::encode(in, sum_of_values, n, encodes[block_optpfor]);
            // auto comparator = [](int x, int y) { return x < y ? x : y; };
            std::vector<size_t> sizes {
                encodes[block_varintg8iu].size(),
                encodes[block_streamvbyte].size(),
                encodes[block_maskedvbyte].size(),
                encodes[block_interpolative].size(),
                encodes[block_qmx].size(),
                encodes[block_simple8b].size(),
                encodes[block_simple16].size(),
                encodes[block_simdbp].size(),
                encodes[block_optpfor].size()
            };

            // Gets codec (index). TODO: test this!
            uint8_t codec = std::min_element(sizes.begin(), sizes.end()) - sizes.begin();
            size_t out_len = encodes[codec].size();
            out.insert(out.end(), encodes[codec].data(), encodes[codec].data() + out_len);
            return codec;
        }

        class document_enumerator {
        public:

            document_enumerator(uint8_t const* data, uint64_t universe,
                                size_t term_id = 0)
                : m_n(0) // just to silence warnings
                , m_base(TightVariableByte::decode(data, &m_n, 1))
                , m_blocks(ceil_div(m_n, block_size))
                , m_block_maxs(m_base)
                , m_block_endpoints(m_block_maxs + 4 * m_blocks)
                , m_blocks_data(m_block_endpoints + 4 * (m_blocks - 1))
                , m_universe(universe)
            {
                if (Profile) {
                    // std::cout << "OPEN\t" << m_term_id << "\t" << m_blocks << "\n";
                    m_block_profile = block_profiler::open_list(term_id, m_blocks);
                }
                m_docs_buf.resize(block_size);
                m_freqs_buf.resize(block_size);
                reset();
            }

            void reset()
            {
                decode_docs_block(0);
            }

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

            uint64_t docid() const
            {
                return m_cur_docid;
            }

            uint64_t PISA_ALWAYSINLINE freq()
            {
                if (!m_freqs_decoded) {
                    decode_freqs_block();
                }
                return m_freqs_buf[m_pos_in_block] + 1;
            }

            uint64_t position() const
            {
                return m_cur_block * block_size + m_pos_in_block;
            }

            uint64_t size() const
            {
                return m_n;
            }

            uint64_t num_blocks() const
            {
                return m_blocks;
            }

            uint64_t stats_freqs_size()
            {
                // XXX rewrite in terms of get_blocks()
                uint64_t bytes = 0;
                uint8_t const* ptr = m_blocks_data;
                // static const uint64_t block_size = block_size;
                std::vector<uint32_t> buf(block_size);
                for (size_t b = 0; b < m_blocks; ++b) {
                    uint32_t cur_block_size =
                        ((b + 1) * block_size <= size())
                        ? block_size : (size() % block_size);

                    uint32_t cur_base = (b ? block_max(b - 1) : uint32_t(-1)) + 1;
                    unpack_codecs(ptr);
                    ptr++;
                    uint8_t const* freq_ptr =
                       decode(cur_doc_codec, ptr, buf.data(),
                              block_max(b) - cur_base - (cur_block_size - 1),
                              cur_block_size);
                    ptr = decode(cur_freq_codec, freq_ptr, buf.data(),
                                 uint32_t(-1), cur_block_size);
                    bytes += ptr - freq_ptr;
                }

                return bytes;
            }

            struct block_data {
                uint32_t index;
                uint32_t max;
                uint32_t size;
                uint32_t doc_gaps_universe;

                void append_docs_block(std::vector<uint8_t>& out) const
                {
                    out.insert(out.end(), docs_begin, freqs_begin);
                }

                void append_freqs_block(std::vector<uint8_t>& out) const
                {
                    out.insert(out.end(), freqs_begin, end);
                }

                void decode_doc_gaps(std::vector<uint32_t>& out) const
                {
                    out.resize(size);
                    decode(docs_begin, out.data(),
                                       doc_gaps_universe, size);
                }

                void decode_freqs(std::vector<uint32_t>& out) const
                {
                    out.resize(size);
                    decode(freqs_begin, out.data(),
                                       uint32_t(-1), size);
                }

            private:
                friend class document_enumerator;

                uint8_t const* docs_begin;
                uint8_t const* freqs_begin;
                uint8_t const* end;
            };

            std::vector<block_data> get_blocks()
            {
                std::vector<block_data> blocks;

                uint8_t const* ptr = m_blocks_data;
                // static const uint64_t block_size = block_size;
                std::vector<uint32_t> buf(block_size);
                for (size_t b = 0; b < m_blocks; ++b) {
                    blocks.emplace_back();
                    uint32_t cur_block_size =
                        ((b + 1) * block_size <= size())
                        ? block_size : (size() % block_size);

                    uint32_t cur_base = (b ? block_max(b - 1) : uint32_t(-1)) + 1;
                    uint32_t gaps_universe = block_max(b) - cur_base - (cur_block_size - 1);

                    blocks.back().index = b;
                    blocks.back().size = cur_block_size;
                    blocks.back().docs_begin = ptr;
                    blocks.back().doc_gaps_universe = gaps_universe;
                    blocks.back().max = block_max(b);

                    unpack_codecs(ptr);
                    ptr++;
                    uint8_t const* freq_ptr =
                        decode(cur_doc_codec, ptr, buf.data(),
                                           gaps_universe, cur_block_size);
                    blocks.back().freqs_begin = freq_ptr;
                    ptr = decode(cur_freq_codec, buf.data(),
                                             uint32_t(-1), cur_block_size);
                    blocks.back().end = ptr;
                }

                assert(blocks.size() == num_blocks());
                return blocks;
            }

        private:

            void unpack_codecs(uint8_t const* block_data)
            {
                uint8_t codec = *block_data++;
                cur_doc_codec = codec & 0b00001111;
                cur_freq_codec = (codec & 0b11110000) >> 4;
            }

            uint32_t block_max(uint32_t block) const
            {
                return ((uint32_t const*)m_block_maxs)[block];
            }

            uint8_t const* decode(uint8_t codec, uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const
            {
                switch(codec) {
                    case block_varintg8iu:
                        in = varint_G8IU_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_streamvbyte:
                        in = streamvbyte_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_maskedvbyte:
                        in = maskedvbyte_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_interpolative:
                        in = interpolative_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_qmx:
                        in = qmx_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_simple8b:
                        in = simple8b_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_simple16:
                        in = simple16_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_simdbp:
                        in = simdbp_block::decode(in, out, sum_of_values, n);
                        break;
                    case block_optpfor:
                        in = optpfor_block::decode(in, out, sum_of_values, n);
                        break;
                }

                // std::cout << "Used codec: " << codec << std::endl;
                return in;
            }

            void PISA_NOINLINE decode_docs_block(uint64_t block)
            {
                // static const uint64_t block_size = block_size;
                uint32_t endpoint = block
                    ? ((uint32_t const*)m_block_endpoints)[block - 1]
                    : 0;
                uint8_t const* block_data = m_blocks_data + endpoint;
                m_cur_block_size =
                    ((block + 1) * block_size <= size())
                    ? block_size : (size() % block_size);
                uint32_t cur_base = (block ? block_max(block - 1) : uint32_t(-1)) + 1;
                m_cur_block_max = block_max(block);
                unpack_codecs(block_data);
                block_data++;
                m_freqs_block_data =
                    decode(cur_doc_codec, block_data, m_docs_buf.data(),
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
                uint8_t const* next_block = decode(cur_freq_codec, m_freqs_block_data, m_freqs_buf.data(),
                                                   uint32_t(-1), m_cur_block_size);
                intrinsics::prefetch(next_block);
                m_freqs_decoded = true;

                if (Profile) {
                    ++m_block_profile[2 * m_cur_block + 1];
                }
            }

            uint32_t m_n;
            uint8_t const* m_base;
            uint32_t m_blocks;
            uint8_t const* m_block_maxs;
            uint8_t const* m_block_endpoints;
            uint8_t const* m_blocks_data;
            uint64_t m_universe;

            uint32_t m_cur_block;
            uint32_t m_pos_in_block;
            uint32_t m_cur_block_max;
            uint32_t m_cur_block_size;
            uint32_t m_cur_docid;
            uint8_t cur_doc_codec;
            uint8_t cur_freq_codec;

            uint8_t const* m_freqs_block_data;
            bool m_freqs_decoded;

            std::vector<uint32_t> m_docs_buf;
            std::vector<uint32_t> m_freqs_buf;

            block_profiler::counter_type* m_block_profile;

       };

    };
}
