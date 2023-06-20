#include <cstddef>
#include <filesystem>

#include <gtest/gtest.h>
#include "air/lightmdb/fixed.hpp"

using namespace air::lightmdb;
constexpr auto FILE_NAME = "table.db";

TEST(fixed_table, fixed_table)
{
    auto table = std::make_unique<fixed::table<size_t>>(FILE_NAME, air::lightmdb::mode_t::create_only, 8);

    ASSERT_EQ(table->size(), 0);
    ASSERT_EQ(table->capacity(), 8);
    ASSERT_TRUE(table->empty());

    for (size_t i = 0; i < 10; i++)
    {
        table->push(i);
        ASSERT_EQ((*table)[i], i);
    }

    ASSERT_EQ(table->size(), 10);
    ASSERT_EQ(table->capacity(), 16);

    ASSERT_TRUE(table->has_value(0));
    ASSERT_FALSE(table->has_value(10));

    table->shrink_to_fit();
    table = std::make_unique<fixed::table<size_t>>(FILE_NAME, air::lightmdb::mode_t::read_only);
    ASSERT_EQ(table->size(), 10);
    ASSERT_EQ(table->capacity(), 10);
    for (size_t i = 0; i < 10; i++)
    {
        ASSERT_EQ((*table)[i], i);
    }

    std::filesystem::remove(FILE_NAME);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}