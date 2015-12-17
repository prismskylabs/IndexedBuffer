#include <gtest/gtest.h>

#include <chrono>

#include "indexed/chrono-snap.h"


TEST(ChronoSnapTests, DifferenceBetweenLessThanThirtySeconds) {
    auto now = std::chrono::system_clock::now();
    auto snapped = prism::indexed::SnapToMinute(now);
    if (snapped > now) {
        EXPECT_LE(snapped - now, std::chrono::seconds(30));
    } else {
        EXPECT_LE(now - snapped, std::chrono::seconds(30));
    }
}

TEST(ChronoSnapTests, SnappedToMinuteReturnsSame) {
    auto now = std::chrono::system_clock::now();
    auto snapped = prism::indexed::SnapToMinute(now);
    EXPECT_EQ(snapped, prism::indexed::SnapToMinute(snapped));
}

TEST(ChronoSnapTests, RoundDownFromAbove) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_LE(prism::indexed::SnapToMinute(snapped + std::chrono::milliseconds(29999)), now);
}

TEST(ChronoSnapTests, RoundUpFromAbove) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_GE(prism::indexed::SnapToMinute(snapped + std::chrono::milliseconds(30000)), now);
}

TEST(ChronoSnapTests, RoundUpFromBelow) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_GE(prism::indexed::SnapToMinute(snapped - std::chrono::milliseconds(30000)),
              now - std::chrono::minutes(1));
}

TEST(ChronoSnapTests, RoundDownFromBelow) {
    auto now = std::chrono::system_clock::now();
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto snapped = std::chrono::system_clock::time_point(
            now - std::chrono::milliseconds(milliseconds.time_since_epoch().count() % 60000));
    EXPECT_LE(prism::indexed::SnapToMinute(snapped - std::chrono::milliseconds(30001)),
              now - std::chrono::minutes(1));
}
