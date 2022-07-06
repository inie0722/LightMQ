#pragma once

#include <cstdint>

#include <filesystem>
#include <fstream>
#include <string_view>

#include "LightMQ/core.hpp"

namespace LightMQ
{
    namespace fixed
    {
        template <typename T>
        class optional
        {
        public:
            using value_type = T;

        private:
            detail::atomic<bool> is_value_;
            value_type value_;

        public:
            optional &operator=(const value_type &val)
            {
                value_ = val;
                is_value_ = true;
                is_value_.notify_all();
                return *this;
            }

            value_type &operator*()
            {
                return value_;
            }

            const value_type &operator*() const
            {
                return value_;
            }

            value_type *operator->()
            {
                return &value_;
            }

            const value_type *operator->() const
            {
                return &value_;
            }

            bool has_value() const
            {
                return is_value_;
            }

            operator bool() const
            {
                return has_value();
            }

            void wait() const
            {
                is_value_.wait(false);
            }

            value_type &value()
            {
                if (!is_value_)
                {
                    throw std::runtime_error("bad row access");
                }

                return is_value_;
            }

            const value_type &value() const
            {
                return const_cast<optional *>(this)->value();
            }
        };

        template <typename T>
        class table
        {
        public:
            using value_type = T;
            using optional_type = optional<value_type>;

        private:
            detail::mmap mmap_;
            optional_type *optional_;

            // 本地 capacity
            std::uint64_t capacity_;

            /// 推入数据
            size_t do_push(const value_type &val, size_t index)
            {
                if (index >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    auto flag = header.lock.exchange(true);

                    if (!flag)
                    {
                        this->recapacity();
                        header.lock = false;
                        header.capacity.notify_all();
                    }

                    header.capacity.wait(capacity_ * sizeof(optional_type));
                    this->remmap();
                }

                optional_[index] = val;

                return index;
            }

            /// 读取数据
            optional_type &do_read(size_t index)
            {
                while (index >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    header.capacity.wait(capacity_ * sizeof(optional_type));
                    capacity_ = header.capacity;
                }
                return optional_[index];
            }

        public:
            table(std::string_view name, mode_t mode, size_t capacity)
                : mmap_(name, mode, capacity)
            {
                optional_ = static_cast<optional_type *>(mmap_.get_address());
                capacity_ = this->capacity();
            }

            table(std::string_view name, mode_t mode)
                : mmap_(name, mode)
            {
                optional_ = static_cast<optional_type *>(mmap_.get_address());
                capacity_ = this->capacity();
            }

            ~table() = default;

            size_t push(const value_type &val)
            {
                auto index = mmap_.get_header().size.fetch_add(1);
                return this->do_push(val, index);
            }

            optional_type &operator[](size_t index)
            {
                return this->do_read(index);
            }

            const optional_type &operator[](size_t index) const
            {
                return const_cast<optional_type *>(this)->do_read(index);
            }

            size_t size() const
            {
                return mmap_.size() / sizeof(optional_type);
            }

            size_t capacity() const
            {
                return mmap_.capacity() / sizeof(optional_type);
            }

            void shrink_to_fit()
            {
                mmap_.shrink_to_fit();
            }
        };
    } // namespace fixed
} // namespace LightMQ