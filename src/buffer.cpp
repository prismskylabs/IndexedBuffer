#include "indexed/buffer.h"

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "indexed/chrono-snap.h"
#include "indexed/database.h"
#include "indexed/filesystem.h"


namespace prism {
namespace indexed {

namespace fs = ::boost::filesystem;

class Buffer::Impl {
  public:
    Impl(const std::string& buffer_root, const double& gigabyte_quota,
         std::function<std::string(void)> hash_function);

    bool Delete(const std::chrono::system_clock::time_point& time_point,
                const unsigned int& device);
    std::string GetBufferDirectory() const;
    std::map<Device, ItemMap> GetCatalog();
    std::string GetFilepath(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device);
    bool Full();
    bool PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device);
    bool SetLowPriority(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device);
    bool KeepIfPossible(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device);
    bool BulkPreserveRecord(const std::vector<std::chrono::system_clock::time_point>& time_points,
                            const unsigned int& device);
    bool BulkSetLowPriority(const std::vector<std::chrono::system_clock::time_point>& time_points,
                            const unsigned int& device);
    bool BulkKeepIfPossible(const std::vector<std::chrono::system_clock::time_point>& time_points,
                            const unsigned int& device);
    bool Push(const std::chrono::system_clock::time_point& time_point, const unsigned int& device,
              const std::string& filepath);
    static std::string MakeHash();

  private:
    bool setKeep(const std::chrono::system_clock::time_point& time_point,
                 const unsigned int& device, const unsigned int& keep);
    bool bulkSetKeep(const std::vector<std::chrono::system_clock::time_point>& time_points,
                     const unsigned int& device, const unsigned int& keep);

    Filesystem filesystem_;
    Database database_;
    std::mutex mutex_;
    std::function<std::string(void)> hash_function_;
};

Buffer::Impl::Impl(const std::string& buffer_root, const double& gigabyte_quota,
                   std::function<std::string(void)> hash_function)
        : filesystem_{"prism_indexed_buffer", buffer_root, gigabyte_quota},
          database_{filesystem_.GetFilepath("prism_indexed_data.db")},
          hash_function_{hash_function} {
    assert(gigabyte_quota > 0);
    srand(std::chrono::system_clock::now().time_since_epoch().count());
}

bool Buffer::Impl::Delete(const std::chrono::system_clock::time_point& time_point,
                          const unsigned int& device) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string hash;
    try {
        hash = database_.FindHash(utility::SnapToMinute(time_point), device);
    } catch (const DatabaseException& e) {
        return false;
    }

    if (hash.empty()) {
        return false;
    }

    filesystem_.Delete(hash);
    try {
        database_.Delete(hash);
    } catch (const DatabaseException& e) {
        return false;
    }
    return true;
}

std::string Buffer::Impl::GetBufferDirectory() const {
    return filesystem_.GetBufferDirectory();
}

std::map<Device, ItemMap> Buffer::Impl::GetCatalog() {
    std::map<Device, ItemMap> catalog;
    auto records = database_.SelectAll();
    for (auto& record : records) {
        Device device = std::stoi(record["device"]);
        auto time_value = std::stoull(record["time_value"]);
        auto hour_bucket =
                std::chrono::system_clock::time_point(std::chrono::hours(time_value / 60));
        catalog[device][hour_bucket].emplace_back(
                Item{static_cast<unsigned int>(time_value % 60)});
    }

    return catalog;
}

std::string Buffer::Impl::GetFilepath(const std::chrono::system_clock::time_point& time_point,
                                      const unsigned int& device) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string hash;

    try {
         hash = database_.FindHash(utility::SnapToMinute(time_point), device);
    } catch (const DatabaseException& e) {
    }

    if (hash.empty()) {
        return hash;
    }

    const auto filepath = filesystem_.GetExistingFilepath(hash);
    if (filepath.empty()) {
        try {
            database_.Delete(hash);
        } catch (const DatabaseException& e) {
        }
    }

    return filepath;
}

bool Buffer::Impl::Full() {
    return filesystem_.AboveQuota();
}

bool Buffer::Impl::PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                                  const unsigned int& device) {
    return setKeep(time_point, device, PRESERVE_RECORD);
}

bool Buffer::Impl::SetLowPriority(const std::chrono::system_clock::time_point& time_point,
                                  const unsigned int& device) {
    return setKeep(time_point, device, DELETE_IF_FULL);
}

bool Buffer::Impl::KeepIfPossible(const std::chrono::system_clock::time_point& time_point,
                                  const unsigned int& device) {
    return setKeep(time_point, device, ATTEMPT_KEEP);
}

bool Buffer::Impl::BulkPreserveRecord(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return bulkSetKeep(time_points, device, PRESERVE_RECORD);
}

bool Buffer::Impl::BulkSetLowPriority(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return bulkSetKeep(time_points, device, DELETE_IF_FULL);
}

bool Buffer::Impl::BulkKeepIfPossible(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return bulkSetKeep(time_points, device, ATTEMPT_KEEP);
}

bool Buffer::Impl::Push(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device, const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (filesystem_.AboveQuota()) {
        std::vector<std::string> hashes;
        std::vector<std::string> deleted_hashes;
        try {
            hashes = database_.GetLowestDeletableHashes();
        } catch (const DatabaseException& e) {
            return false;
        }

        for (const auto& hash : hashes) {
            if (!filesystem_.AboveQuota()) {
                break;
            }

            if (hash.empty()) {
                fs::remove(filepath);
                return false;
            }

            filesystem_.Delete(hash);
            deleted_hashes.push_back(hash);
        }

        try {
            database_.BulkDelete(deleted_hashes);
        } catch (const DatabaseException& e) {
            return false;
        }
    }

    if (!fs::exists(filepath) || fs::is_directory(filepath)) {
        return false;
    }

    auto size = fs::file_size(filepath);
    auto hash = hash_function_();

    if (filesystem_.Move(filepath, hash)) {
        try {
            database_.Insert(utility::SnapToMinute(time_point), device, hash, size, ATTEMPT_KEEP);
        } catch (const DatabaseException& e) {
            filesystem_.Delete(hash);
        }
    } else {
        fs::remove(filepath);
    }
    return true;
}

std::string Buffer::Impl::MakeHash() {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    std::stringstream stream;
    for (int i = 0; i < 32; ++i) {
        stream << alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return stream.str();
}

bool Buffer::Impl::setKeep(const std::chrono::system_clock::time_point& time_point,
                           const unsigned int& device, const unsigned int& keep) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        return database_.SetKeep(utility::SnapToMinute(time_point), device, keep);
    } catch (const DatabaseException& e) {
        return false;
    }
    return false;
}

bool Buffer::Impl::bulkSetKeep(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device, const unsigned int& keep) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<unsigned long long> minutes;
    for (const auto& time_point : time_points) {
        minutes.emplace_back(utility::SnapToMinute(time_point));
    }

    try {
        return database_.BulkSetKeep(minutes, device, keep);
    } catch (const DatabaseException& e) {
        return false;
    }
    return false;
}

// Bridge

Buffer::Buffer() : Buffer(std::string{}, 2.0) {}

Buffer::Buffer(const std::string& buffer_root) : Buffer(buffer_root, 2.0) {}

Buffer::Buffer(const std::string& buffer_root, const double& gigabyte_quota)
        : Buffer(buffer_root, gigabyte_quota, Buffer::Impl::MakeHash) {}

Buffer::Buffer(const std::string& buffer_root, const double& gigabyte_quota,
               std::function<std::string(void)> hash_function)
        : impl_{new Impl{buffer_root, gigabyte_quota, hash_function}} {}

Buffer::~Buffer() {}

bool Buffer::Delete(const std::chrono::system_clock::time_point& time_point,
                    const unsigned int& device) {
    return impl_->Delete(time_point, device);
}

std::string Buffer::GetBufferDirectory() const {
    return impl_->GetBufferDirectory();
}

std::map<Device, ItemMap> Buffer::GetCatalog() {
    return impl_->GetCatalog();
}

std::string Buffer::GetFilepath(const std::chrono::system_clock::time_point& time_point,
                                const unsigned int& device) {
    return impl_->GetFilepath(time_point, device);
}

bool Buffer::Full() {
    return impl_->Full();
}

bool Buffer::PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device) {
    return impl_->PreserveRecord(time_point, device);
}

bool Buffer::SetLowPriority(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device) {
    return impl_->SetLowPriority(time_point, device);
}

bool Buffer::KeepIfPossible(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device) {
    return impl_->KeepIfPossible(time_point, device);
}

bool Buffer::BulkPreserveRecord(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return impl_->BulkPreserveRecord(time_points, device);
}

bool Buffer::BulkSetLowPriority(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return impl_->BulkSetLowPriority(time_points, device);
}

bool Buffer::BulkKeepIfPossible(
        const std::vector<std::chrono::system_clock::time_point>& time_points,
        const unsigned int& device) {
    return impl_->BulkKeepIfPossible(time_points, device);
}

bool Buffer::Push(const std::chrono::system_clock::time_point& time_point,
                  const unsigned int& device, const std::string& filepath) {
    return impl_->Push(time_point, device, filepath);
}

} // namespace indexed
} // namespace prism
