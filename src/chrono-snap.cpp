#include "indexed/chrono-snap.h"

#include <chrono>


namespace prism {
namespace indexed {

Minutes SnapToMinute(const std::chrono::system_clock::time_point& time_point) {
    auto milliseconds = std::chrono::time_point_cast<std::chrono::milliseconds>(time_point);
    if (milliseconds.time_since_epoch().count() % 60000 < 30000) {
        return std::chrono::time_point_cast<std::chrono::minutes>(time_point);
    } else {
        return std::chrono::time_point_cast<std::chrono::minutes>(time_point +
                                                                  std::chrono::seconds(30));
    }
}

} // namespace indexed
} // namespace prism
