#include "indexed/chrono-snap.h"

#include <chrono>


namespace prism {
namespace indexed {

unsigned long long SnapToMinute(const std::chrono::system_clock::time_point& time_point) {
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(time_point);
    if (milliseconds.time_since_epoch().count() % 60000 < 30000) {
        return std::chrono::time_point_cast<std::chrono::minutes>(time_point)
                .time_since_epoch()
                .count();
    } else {
        return std::chrono::time_point_cast<std::chrono::minutes>(time_point +
                                                                  std::chrono::seconds(30))
                .time_since_epoch()
                .count();
    }
}

} // namespace indexed
} // namespace prism
