#include "indexed/chrono-snap.h"

#include <chrono>


namespace prism {
namespace indexed {
namespace utility {

unsigned long long SnapToMinute(const std::chrono::system_clock::time_point& time_point) {
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(time_point);
    return std::chrono::time_point_cast<std::chrono::minutes>(time_point + std::chrono::seconds(30))
            .time_since_epoch()
            .count();
}

} // namespace utility
} // namespace indexed
} // namespace prism
