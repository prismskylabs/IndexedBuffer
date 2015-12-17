#ifndef PRISM_INDEXED_CHRONO_SNAP_H_
#define PRISM_INDEXED_CHRONO_SNAP_H_

#include <chrono>


namespace prism {
namespace indexed {

using Minutes = std::chrono::time_point<std::chrono::system_clock, std::chrono::minutes>;

Minutes SnapToMinute(const std::chrono::system_clock::time_point& time_point);

} // namespace indexed
} // namespace prism

#endif /* PRISM_INDEXED_CHRONO_SNAP_H_ */
