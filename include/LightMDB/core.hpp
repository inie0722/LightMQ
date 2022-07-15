#pragma once

#include <memory>
#include <cstddef>
#include <string_view>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <climits>
#include <limits>

#include <boost/atomic/ipc_atomic.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>

namespace LightMDB
{
    enum class mode_t : int8_t
    {
        create_only = 0,
        open_or_create,
        read_write,
        read_only,
        read_private,
        copy_on_write
    };

    namespace detail
    {
        template <typename T>
        using atomic = boost::ipc_atomic<T>;

        class mmap
        {
        public:
            using size_type = std::size_t;
            using difference_type = std::ptrdiff_t;

            struct header
            {
                detail::atomic<size_type> size;
                detail::atomic<size_type> capacity;
                detail::atomic<bool> lock;
            };

        private:
            header *header_;
            std::string mmap_name_;
            std::unique_ptr<boost::interprocess::file_mapping> file_mapp_;
            std::unique_ptr<boost::interprocess::mapped_region> region_;

            void create_file(size_type size)
            {
                std::filebuf fbuf;
                fbuf.open(mmap_name_, std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
                fbuf.pubseekoff(size - 1, std::ios::beg);
                fbuf.sputc(0);
            }

            void create_only(size_type capacity)
            {
                using namespace boost::interprocess;

                this->create_file(sizeof(header) + capacity);

                file_mapp_ = std::make_unique<file_mapping>(mmap_name_.c_str(), boost::interprocess::mode_t::read_write);
                region_ = std::make_unique<mapped_region>(*file_mapp_, boost::interprocess::mode_t::read_write);

                header_ = new (region_->get_address()) header;
                header_->size = 0;
                header_->lock = false;
                header_->capacity = capacity;
            }

            void open_only(boost::interprocess::mode_t file_mapping_mode, boost::interprocess::mode_t mapped_region_mode)
            {
                using namespace boost::interprocess;

                file_mapp_ = std::make_unique<file_mapping>(mmap_name_.c_str(), file_mapping_mode);
                region_ = std::make_unique<mapped_region>(*file_mapp_, mapped_region_mode);
                header_ = static_cast<header *>(region_->get_address());
            }

        public:
            mmap(std::string_view name, mode_t mode, size_type capacity)
                : mmap_name_(name)
            {
                switch (mode)
                {
                case mode_t::create_only:
                    create_only(capacity);
                    break;
                case mode_t::open_or_create:
                    if (std::filesystem::exists(name))
                        open_only(boost::interprocess::mode_t::read_write, boost::interprocess::mode_t::read_write);
                    else
                        create_only(capacity);
                    break;
                default:
                    throw std::runtime_error("error mode");
                }
            }

            mmap(std::string_view name, mode_t mode)
                : mmap_name_(name)
            {
                switch (mode)
                {
                case mode_t::read_write:
                    open_only(boost::interprocess::mode_t::read_write, boost::interprocess::mode_t::read_write);
                    break;
                case mode_t::read_only:
                    open_only(boost::interprocess::mode_t::read_only, boost::interprocess::mode_t::read_only);
                    break;
                case mode_t::read_private:
                    open_only(boost::interprocess::mode_t::read_only, boost::interprocess::mode_t::read_private);
                    break;
                case mode_t::copy_on_write:
                    open_only(boost::interprocess::mode_t::read_only, boost::interprocess::mode_t::copy_on_write);
                    break;
                default:
                    throw std::runtime_error("error mode");
                }
            }

            ~mmap() = default;

            size_type size() const
            {
                return header_->size;
            }

            size_type max_size() const
            {
                return std::numeric_limits<size_type>::max();
            }

            size_type reserve() const
            {
                return this->capacity() - this->size();
            }

            size_type capacity() const
            {
                return region_->get_size() - sizeof(header);
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
} // namespace LightMDB
