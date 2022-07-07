#include <iostream>
#include <thread>
#include <cstddef>
#include <vector>

#include <gtest/gtest.h>
#include "LightMQ/variable.hpp"

constexpr size_t COUNT = 10000;

constexpr size_t BUFFER_SIZE = 256;

constexpr size_t THREAD_WRITE_NUM = 2;

constexpr size_t THREAD_READ_NUM = 2;

class verify
{
public:
    template <size_t DATA_SIZE>
    struct value
    {
        size_t val;
        char _[DATA_SIZE - sizeof(val)];
    };

    template <size_t DATA_SIZE>
    void run_one()
    {
        using value_t = value<DATA_SIZE>;
        LightMQ::variable::table("table.db", LightMQ::mode_t::create_only, BUFFER_SIZE, BUFFER_SIZE);

        std::vector<std::atomic<size_t>> array(COUNT);

        std::atomic<size_t> write_diff = 0;
        std::atomic<size_t> read_diff = 0;

        std::thread write_thread[THREAD_WRITE_NUM];
        std::thread read_thread[THREAD_READ_NUM];

        for (size_t i = 0; i < THREAD_WRITE_NUM; i++)
        {
            write_thread[i] = std::thread([&]()
                                          {
                LightMQ::variable::table table("table.db", LightMQ::mode_t::open_read_write);
                value_t data;

                auto start = std::chrono::steady_clock::now();
                for (size_t i = 0; i < COUNT; i++)
                {
                    data.val = i;
                    table.push(&data, sizeof(data));    
                }
                auto end = std::chrono::steady_clock::now();
                write_diff += (end - start).count(); });
        }

        for (size_t i = 0; i < THREAD_READ_NUM; i++)
        {
            read_thread[i] = std::thread([&]()
                                         {
                LightMQ::variable::table table("table.db", LightMQ::mode_t::open_read_write);
                value_t data;

                auto start = std::chrono::steady_clock::now();
                for (size_t i = 0; i < COUNT * THREAD_WRITE_NUM; i++)
                {
                    table.wait(i);
                    data = *(value_t*)table[i].first;
                    size_t index = data.val;
                    array[index]++;
                }
                auto end = std::chrono::steady_clock::now();
                read_diff += (end - start).count(); });
        }

        for (size_t i = 0; i < THREAD_WRITE_NUM; i++)
        {
            write_thread[i].join();
        }

        for (size_t i = 0; i < THREAD_READ_NUM; i++)
        {
            read_thread[i].join();
        }

        size_t max = 0;
        size_t min = 0;
        for (size_t i = 0; i < COUNT; i++)
        {
            if (array[i] != THREAD_WRITE_NUM * THREAD_READ_NUM)
            {
                if (array[i] > THREAD_WRITE_NUM * THREAD_READ_NUM)
                    max++;
                else
                    min++;
            }
        }

        std::cout << "size/" << DATA_SIZE << " byte\t"
                  << "w/" << (write_diff / THREAD_WRITE_NUM / COUNT)
                  << " ns\t"
                  << "r/" << read_diff / THREAD_READ_NUM / COUNT
                  << " ns\t" << std::endl;

        ASSERT_TRUE(max == 0);
        ASSERT_TRUE(min == 0);
    }

    template <size_t... DATA_SIZE_>
    void run()
    {
        (run_one<DATA_SIZE_>(), ...);
    }
};

TEST(variable_table, variable_table)
{
    LightMQ::variable::table table("table.db", LightMQ::mode_t::create_only, 8, 8);

    int a = rand();
    table.push(&a, sizeof(a));

    ASSERT_TRUE(table.size().first == 1);

    int b = *(int*)table[0].first;
    ASSERT_TRUE(b == a);

    int array_a[5] = {rand(), rand(), rand(), rand(), rand()};

    for (size_t i = 0; i < 5; i++)
    {
        table.push(&array_a[i], sizeof(int));
    }

    ASSERT_TRUE(table.size().first == 6);

    int array_b[5] = {0};

    for (size_t i = 0; i < 6; i++)
    {
        array_b[i] =  *(int*)table[i + 1].first;
    }

    ASSERT_TRUE(0 == memcmp(array_a, array_b, sizeof(array_a)));
}

TEST(variable_table, performance)
{
    verify v;
    v.run<64, 128, 256, 512, 1024>();
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}