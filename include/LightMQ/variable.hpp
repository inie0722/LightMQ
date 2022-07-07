#pragma once

#include <atomic>
#include <string_view>
#include <cstddef>
#include <utility>

#include "LightMQ/core.hpp"
#include "LightMQ/fixed.hpp"

namespace LightMQ
{
    namespace variable
    {
        class table
        {
        private:
            fixed::table<std::pair<std::size_t, std::size_t>> offset_db_;
            detail::mmap mmap_;
            char *data_;

            // 本地 capacity
            std::size_t capacity_;

            void remmap()
            {
                capacity_ = mmap_.get_header().capacity;
                mmap_.remmap();
                data_ = static_cast<char *>(mmap_.get_address());
            }

            /// 推入数据
            std::size_t do_push(const void *val, std::size_t size, std::size_t index)
            {

                while (index + size >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    auto flag = header.lock.exchange(true);

                    if (!flag)
                    {
                        if (capacity_ == this->capacity().second)
                        {
                            mmap_.recapacity();
                        }

                        header.lock = false;
                        header.capacity.notify_all();
                    }

                    header.capacity.wait(capacity_);
                    this->remmap();
                }

                memcpy(&data_[index], val, size);
                return index;
            }

            void *do_read(std::size_t index, std::size_t size)
            {
                while (index + size >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    header.capacity.wait(capacity_);

                    this->remmap();
                }
                return &data_[index];
            }

        public:
            table(const std::string &name, mode_t mode, std::size_t capacity, std::size_t index_capacity)
                : offset_db_(name + ".idb", mode, index_capacity), mmap_(name, mode, capacity)
            {
                data_ = static_cast<char *>(mmap_.get_address());
                capacity_ = this->capacity().second;
            }

            table(const std::string &name, mode_t mode)
                : offset_db_(name + ".idb", mode), mmap_(name, mode)
            {
                data_ = static_cast<char *>(mmap_.get_address());
                capacity_ = this->capacity().second;
            }

            ~table() = default;

            std::size_t push(const void *val, std::size_t size)
            {
                auto index = mmap_.get_header().size.fetch_add(size);
                return offset_db_.push({this->do_push(val, size, index), size});
            }

            bool has_value(std::size_t index)
            {
                return offset_db_.has_value(index);
            }

            std::pair<void *, std::size_t> operator[](std::size_t index)
            {
                auto offset = offset_db_[index];
                return {do_read(offset.first, offset.second), offset.second};
            }

            std::pair<const void *, std::size_t> operator[](std::size_t index) const
            {
                return const_cast<table *>(this)->operator[](index);
            }

            void wait(std::size_t index) const
            {
                offset_db_.wait(index);
            }

            std::pair<std::size_t, std::size_t> size() const
            {
                return {offset_db_.size(), mmap_.size()};
            }

            std::pair<std::size_t, std::size_t> capacity() const
            {
                return {offset_db_.capacity(), mmap_.capacity()};
            }

            void shrink_to_fit()
            {
                mmap_.shrink_to_fit();
            }
        };
    } // namespace variable
} // namespace LightMQ