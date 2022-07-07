#pragma once

#include <atomic>
#include <string_view>

#include "LightMQ/core.hpp"

namespace LightMQ
{
    namespace fixed
    {
        template <typename T>
        class table
        {
        public:
            using value_type = T;

        private:
            struct node
            {
                detail::atomic<bool> is_value;
                value_type value;

                node& operator =(const value_type & val)
                {
                    this->value = val;
                    this->is_value = true;
                    return *this;
                }
            
                void wait()
                {
                    this->is_value.wait(false);
                }
            };

            detail::mmap mmap_;
            node *node_;

            // 本地 capacity
            std::uint64_t capacity_;

            void remmap()
            {
                capacity_ = mmap_.get_header().capacity / sizeof(node);
                mmap_.remmap();
                node_ = static_cast<node *>(mmap_.get_address());
            }

            /// 推入数据
            size_t do_push(const value_type &val, size_t index)
            {
                if (index >= capacity_)
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

                    header.capacity.wait(capacity_ / sizeof(node));
                    this->remmap();
                }

                node_[index] = val;
                return index;
            }

            /// 读取数据
            node &do_read(size_t index)
            {
                while (index >= capacity_)
                {
                    auto &header = mmap_.get_header();
                    header.capacity.wait(capacity_ / sizeof(node));

                    this->remmap();
                }
                return node_[index];
            }

        public:
            table(std::string_view name, mode_t mode, size_t capacity)
                : mmap_(name, mode, capacity * sizeof(node))
            {
                node_ = static_cast<node *>(mmap_.get_address());
                capacity_ = this->capacity();
            }

            table(std::string_view name, mode_t mode)
                : mmap_(name, mode)
            {
                node_ = static_cast<node *>(mmap_.get_address());
                capacity_ = this->capacity();
            }

            ~table() = default;

            size_t push(const value_type &val)
            {
                auto index = mmap_.get_header().size.fetch_add(1);
                return this->do_push(val, index);
            }

            value_type &operator[](size_t index)
            {
                return this->do_read(index).value;
            }

            bool has_value(size_t index)
            {
                return this->do_read(index).is_value;
            }

            void wait(size_t index)
            {
                this->do_read(index).wait();
            }

            size_t size() const
            {
                return mmap_.size() / sizeof(node);
            }

            size_t capacity() const
            {
                return mmap_.capacity() / sizeof(node);
            }

            void shrink_to_fit()
            {
                mmap_.shrink_to_fit();
            }
        };
    } // namespace fixed
} // namespace LightMQ