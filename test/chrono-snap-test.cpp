#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>

#include "indexed/chrono-snap.h"


TEST(ChronoSnapTests, DifferenceBetweenLessThanThirtySeconds) {
    auto now = std::chrono::system_clock::now();
    auto snapped = prism::indexed::SnapToMinute(now);
    auto now_seconds = now.time_since_epoch().count() / 6e7;
    EXPECT_GT(30, abs(snapped - now_seconds));
}

TEST(ChronoSnapTests, RoundDownFromAbove) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_LE(prism::indexed::SnapToMinute(snapped + std::chrono::milliseconds(29999)),
              now.time_since_epoch().count() / 6e7);
}

TEST(ChronoSnapTests, RoundUpFromAbove) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_GE(prism::indexed::SnapToMinute(snapped + std::chrono::milliseconds(30000)),
              now.time_since_epoch().count() / 6e7);
}

TEST(ChronoSnapTests, RoundUpFromBelow) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_GE(prism::indexed::SnapToMinute(snapped - std::chrono::milliseconds(30000)),
              (now - std::chrono::minutes(1)).time_since_epoch().count() / 6e7);
}

TEST(ChronoSnapTests, RoundDownFromBelow) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_LE(prism::indexed::SnapToMinute(snapped - std::chrono::milliseconds(30001)),
              (now - std::chrono::minutes(1)).time_since_epoch().count() / 6e7);
}
