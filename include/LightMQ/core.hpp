#pragma once

#include <memory>
#include <cstdint>
#include <bit>
#include <string_view>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <climits>

#include <boost/atomic/ipc_atomic.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>

namespace LightMQ
{
    enum class mode_t : int8_t
    {
        create_only = 0,
        open_read_write,
        open_read_only,
        open_or_create,
        open_copy_on_write
    };

    namespace detail
    {
        template <typename T>
        using atomic = boost::ipc_atomic<T>;

        template <typename T>
        T swap_endian(T u)
        {
            static_assert(CHAR_BIT == 8, "CHAR_BIT != 8");

            union
            {
                T u;
                unsigned char u8[sizeof(T)];
            } source, dest;

            source.u = u;

            for (size_t k = 0; k < sizeof(T); k++)
                dest.u8[k] = source.u8[sizeof(T) - k - 1];

            return dest.u;
        }

        class mmap
        {
        public:
            struct header
            {
                std::endian endian;
                detail::atomic<std::uint64_t> size;
                detail::atomic<std::uint64_t> capacity;
                detail::atomic<bool> lock;
            };

        private:
            header *header_;
            std::string mmap_name_;
            std::unique_ptr<boost::interprocess::file_mapping> file_mapp_;
            std::unique_ptr<boost::interprocess::mapped_region> region_;

            void create_file(size_t size)
            {
                std::filebuf fbuf;
                fbuf.open(mmap_name_, std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
                fbuf.pubseekoff(size - 1, std::ios::beg);
                fbuf.sputc(0);
            }

            void create_only(size_t capacity)
            {
                using namespace boost::interprocess;

                this->create_file(sizeof(header) + capacity);

                file_mapp_ = std::make_unique<file_mapping>(mmap_name_.c_str(), boost::interprocess::mode_t::read_write);
                region_ = std::make_unique<mapped_region>(*file_mapp_, boost::interprocess::mode_t::read_write);

                header_ = new (region_->get_address()) header;
                header_->endian = std::endian::native;
                header_->size = 0;
                header_->lock = false;
                header_->capacity = capacity;
            }

            void open_only(boost::interprocess::mode_t mode)
            {
                using namespace boost::interprocess;

                file_mapp_ = std::make_unique<file_mapping>(mmap_name_.c_str(), mode);
                region_ = std::make_unique<mapped_region>(*file_mapp_, mode);
                header_ = static_cast<header *>(region_->get_address());

                //需要转换字节序
                if (header_->endian == std::endian::native)
                {
                    if (mode == boost::interprocess::read_write)
                    {
                        throw std::runtime_error("Different endian does not support open_read_write");
                    }

                    auto n_header_ = new header;
                    n_header_->endian = header_->endian;
                    n_header_->size = swap_endian(header_->size.load());
                    n_header_->capacity = swap_endian(header_->capacity.load());
                    n_header_->lock = header_->lock.load();

                    header_ = n_header_;
                }
            }

        public:
            mmap(std::string_view name, mode_t mode, std::uint64_t capacity)
            {
                switch (mode)
                {
                case mode_t::create_only:
                    create_only(capacity);
                    break;
                case mode_t::open_or_create:
                    if (std::filesystem::exists(name))
                        open_only(boost::interprocess::mode_t::read_write);
                    else
                        create_only(capacity);
                    break;
                default:
                    throw std::runtime_error("error mode");
                }
            }

            mmap(std::string_view name, mode_t mode)
            {
                switch (mode)
                {
                case mode_t::open_read_write:
                    open_only(boost::interprocess::mode_t::read_write);
                    break;
                case mode_t::open_read_only:
                    open_only(boost::interprocess::mode_t::read_only);
                    break;
                case mode_t::open_copy_on_write:
                    open_only(boost::interprocess::mode_t::copy_on_write);
                    break;
                default:
                    throw std::runtime_error("error mode");
                }
            }

            ~mmap()
            {
                if (header_->endian == std::endian::native)
                {
                    delete header_;
                }
            }

            size_t capacity() const
            {
                return region_->get_size() - sizeof(header);
            }

            size_t size() const
            {
                return header_->size;
            }

            void recapacity()
            {
                std::filesystem::resize_file(mmap_name_, header_->capacity * 2 + sizeof(header));
                header_->capacity = header_->capacity * 2;
            }

            void remmap()
            {
                using namespace boost::interprocess;

                region_->~mapped_region();
                new (region_.get()) mapped_region(*file_mapp_, read_write);
                header_ = static_cast<header *>(region_->get_address());
            }

            void shrink_to_fit()
            {
                std::filesystem::resize_file(mmap_name_, sizeof(header) + header_->size);
                header_->capacity = header_->size.load();
            }

            header &get_header()
            {
                return *this->header_;
            }

            void *get_address()
            {
                return &static_cast<header *>(region_->get_address())[1];
            }
        };
    } // namespace detail
} // namespace LightMQ
