#pragma once

#include <atomic>
#include <string_view>
#include <cstddef>
#include <utility>

#include "air/lightmdb/core.hpp"
#include "air/lightmdb/fixed.hpp"

namespace air
{
    namespace lightmdb
    {
        namespace variable
        {
            template <bool IsLock = true>
            class table
            {
            public:
                using size_type = std::size_t;
                using difference_type = std::ptrdiff_t;

            private:
                fixed::table<std::pair<size_type, size_type>, IsLock> offset_db_;
                detail::mmap mmap_;
                char *data_;

                // 本地 capacity
                size_type capacity_;

                void remmap()
                {
                    capacity_ = mmap_.get_header().capacity;
                    mmap_.remmap();
                    data_ = static_cast<char *>(mmap_.get_address());
                }

                /// 推入数据
                size_type do_push(const void *val, size_type size, size_type index)
                {
                    while (index + size > capacity_)
                    {
                        auto &header = mmap_.get_header();

                        if constexpr (IsLock == true)
                        {
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
                        }
                        else
                        {
                            mmap_.recapacity();
                            header.capacity.wait(capacity_);
                        }
                        this->remmap();
                    }

                    memcpy(&data_[index], val, size);
                    return index;
                }

                void *do_read(size_type index, size_type size)
                {
                    while (index + size > capacity_)
                    {
                        auto &header = mmap_.get_header();
                        header.capacity.wait(capacity_);

                        this->remmap();
                    }
                    return &data_[index];
                }

            public:
                table(const std::string &name, mode_t mode, size_type capacity, size_type index_capacity)
                    : offset_db_(name + "i", mode, index_capacity), mmap_(name, mode, capacity)
                {
                    data_ = static_cast<char *>(mmap_.get_address());
                    capacity_ = this->capacity().second;
                }

                table(const std::string &name, mode_t mode)
                    : offset_db_(name + "i", mode), mmap_(name, mode)
                {
                    data_ = static_cast<char *>(mmap_.get_address());
                    capacity_ = this->capacity().second;
                }

                ~table() = default;

                size_type push(const void *val, size_type size)
                {
                    auto index = mmap_.get_header().size.fetch_add(size);
                    return offset_db_.push({this->do_push(val, size, index), size});
                }

                bool has_value(size_type index)
                {
                    return offset_db_.has_value(index);
                }

                std::pair<void *, size_type> operator[](size_type index)
                {
                    auto offset = offset_db_[index];
                    return {do_read(offset.first, offset.second), offset.second};
                }

                std::pair<void *, size_type> operator[](const std::pair<size_type, size_type> &index)
                {
                    return {do_read(index.first, index.second), index.second};
                }

                std::pair<const void *, size_type> operator[](size_type index) const
                {
                    return const_cast<table *>(this)->operator[](index);
                }

                std::pair<const void *, size_type> operator[](const std::pair<size_type, size_type> &index) const
                {
                    return const_cast<table *>(this)->operator[](index);
                }

                void wait(size_type index) const
                {
                    offset_db_.wait(index);
                }

                bool empty() const
                {
                    return this->offset_db_.empty();
                }

                std::pair<size_type, size_type> size() const
                {
                    return {offset_db_.size(), mmap_.size()};
                }

                std::pair<size_type, size_type> max_size() const
                {
                    return {offset_db_.max_size(), mmap_.max_size()};
                }

                std::pair<size_type, size_type> capacity() const
                {
                    return {offset_db_.capacity(), mmap_.capacity()};
                }

                void shrink_to_fit()
                {
                    offset_db_.shrink_to_fit();
                    mmap_.shrink_to_fit();
                }

                fixed::table<std::pair<size_type, size_type>, IsLock> &index_table()
                {
                    return offset_db_;
                }

                const fixed::table<std::pair<size_type, size_type>, IsLock> &index_table() const
                {
                    return offset_db_;
                }

                std::pair<const std::string &, const std::string &> name() const
                {
                    return {offset_db_.name(), mmap_.name()};
                }
            };
        }
    }
}