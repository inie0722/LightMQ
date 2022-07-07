#pragma once

#include <atomic>
#include <string_view>

#include "LightMQ/core.hpp"
#include "LightMQ/fixed.hpp"

namespace LightMQ
{
    namespace variable
    {
        class table
        {
        private:
            fixed::table<std::pair<std::uint64_t, std::uint64_t>> offset_db_;
            detail::mmap mmap_;
            char *data_;

            // 本地 capacity
            std::uint64_t capacity_;

            void remmap()
            {
                capacity_ = mmap_.get_header().capacity;
                mmap_.remmap();
                data_ = static_cast<char *>(mmap_.get_address());
            }

            /// 推入数据
            size_t do_push(const void *val, size_t size, size_t index)
            {

                while (index + size >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    auto flag = header.lock.exchange(true);

                    if (!flag)
                    {
                        if (capacity_ == this->capacity())
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

        public:
            table(const std::string &name, mode_t mode, size_t capacity, size_t index_capacity)
                : offset_db_(name + ".idb", mode, index_capacity), mmap_(name, mode, capacity)
            {
                data_ = static_cast<char *>(mmap_.get_address());
                capacity_ = this->capacity();
            }

            ~table() = default;
            /// 读取数据
            char *do_read(size_t index, size_t size)
            {
                while (index + size >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    header.capacity.wait(capacity_);

                    this->remmap();
                }
                return &data_[index];
            }

            size_t push(const void *val, size_t size)
            {
                auto index = mmap_.get_header().size.fetch_add(size);
                return offset_db_.push({this->do_push(val, size, index), size});
            }

            bool has_value(size_t index)
            {
                return offset_db_.has_value(index);
            }

            std::pair<void *, size_t> operator[](size_t index)
            {
                auto offset = offset_db_[index];
                return {&data_[offset.first], offset.second};
            }

            void wait(size_t index)
            {
                offset_db_.wait(index);
            }

            size_t size() const
            {
                return mmap_.size();
            }

            size_t capacity() const
            {
                return mmap_.capacity();
            }

            void shrink_to_fit()
            {
                mmap_.shrink_to_fit();
            }
        };
    } // namespace variable
} // namespace LightMQ