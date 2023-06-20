#include <thread>
#include <filesystem>
#include <array>

#include <benchmark/benchmark.h>

#include "air/lightmdb/fixed.hpp"

using namespace air::lightmdb;
constexpr auto FILE_NAME = "table.db";
static auto THREADS = 16 > std::thread::hardware_concurrency() ? 16 : std::thread::hardware_concurrency();

template <size_t I>
static void DoSetup(const benchmark::State &state)
{
    auto file = std::to_string(state.threads()) + FILE_NAME;
    fixed::table<std::array<char, I>> table(file, air::lightmdb::mode_t::create_only, 1024);
}

static void DoTeardown(const benchmark::State &state)
{
    auto file = std::to_string(state.threads()) + FILE_NAME;
    std::filesystem::remove(file);
}

template <size_t I>
static void fixed_table(benchmark::State &state)
{
    auto file = std::to_string(state.threads()) + FILE_NAME;
    fixed::table<std::array<char, I>> table(file, air::lightmdb::mode_t::read_write);
    std::array<char, I> i;
    for (auto _ : state)
    {
        auto c = table.push(i);
        i[0] += 1;
    }
}
BENCHMARK(fixed_table<8>)->ThreadRange(1, THREADS)->Setup(DoSetup<8>)->Teardown(DoTeardown);
BENCHMARK(fixed_table<16>)->ThreadRange(1, THREADS)->Setup(DoSetup<16>)->Teardown(DoTeardown);
BENCHMARK(fixed_table<32>)->ThreadRange(1, THREADS)->Setup(DoSetup<32>)->Teardown(DoTeardown);
BENCHMARK(fixed_table<64>)->ThreadRange(1, THREADS)->Setup(DoSetup<64>)->Teardown(DoTeardown);

BENCHMARK_MAIN();
