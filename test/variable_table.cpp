#include <cstddef>
#include <cstdint>
#include <filesystem>

#include <gtest/gtest.h>
#include "air/lightmdb/variable.hpp"

using namespace air::lightmdb;
constexpr auto FILE_NAME = "table.db";

TEST(fixed_table, fixed_table)
{
    auto table = std::make_unique<variable::table<>>(FILE_NAME, air::lightmdb::mode_t::create_only, 16, 8);

    ASSERT_EQ(table->size().first, 0);
    ASSERT_EQ(table->size().second, 0);
    ASSERT_EQ(table->capacity().first, 8);
    ASSERT_EQ(table->capacity().second, 16);
    ASSERT_TRUE(table->empty());

    for (int64_t i = 0; i < 10; i++)
    {
        table->push(&i, sizeof(i));
        ASSERT_EQ((*table)[i].second, sizeof(i));
        ASSERT_EQ(*(int64_t *)(*table)[i].first, i);
    }

    ASSERT_EQ(table->size().first, 10);
    ASSERT_EQ(table->size().second, 10 * sizeof(int64_t));
    ASSERT_EQ(table->capacity().first, 16);
    ASSERT_EQ(table->capacity().second, 128);

    ASSERT_TRUE(table->has_value(0));
    ASSERT_FALSE(table->has_value(10));

    table->shrink_to_fit();
    table = std::make_unique<variable::table<>>(FILE_NAME, air::lightmdb::mode_t::read_only);
    ASSERT_EQ(table->size().first, 10);
    ASSERT_EQ(table->size().second, 10 * sizeof(int64_t));
    ASSERT_EQ(table->capacity().first, 10);
    ASSERT_EQ(table->capacity().second, 10 * sizeof(int64_t));
    for (int64_t i = 0; i < 10; i++)
    {
        ASSERT_EQ((*table)[i].second, sizeof(i));
        ASSERT_EQ(*(int64_t *)(*table)[i].first, i);
    }

    std::filesystem::remove(FILE_NAME);
    std::filesystem::remove(std::string(FILE_NAME) + "i");
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}