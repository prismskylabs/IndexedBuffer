#include "indexed/buffer.h"

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>

#include <boost/filesystem.hpp>

#include "indexed/chrono-snap.h"
#include "indexed/database.h"
#include "indexed/filesystem.h"


namespace prism {
namespace indexed {

namespace fs = ::boost::filesystem;

class Buffer::Impl {
  public:
    Impl(const std::string& buffer_root, const double& gigabyte_quota);

    bool Delete(const std::chrono::system_clock::time_point& time_point,
                const unsigned int& device);
    std::string GetFilepath(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device);
    bool Full() const;
    bool PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device);
    bool Push(const std::chrono::system_clock::time_point& time_point, const unsigned int& device,
              const std::string& filepath);

  private:
    static std::string makeHash(const int& len = 32);

    Filesystem filesystem_;
    Database database_;
};

Buffer::Impl::Impl(const std::string& buffer_root, const double& gigabyte_quota)
        : filesystem_{"prism_indexed_buffer", buffer_root, gigabyte_quota},
          database_{filesystem_.GetFilepath("prism_indexed_data.db")} {
    assert(gigabyte_quota > 0);
    srand(std::chrono::system_clock::now().time_since_epoch().count());
}

bool Buffer::Impl::Delete(const std::chrono::system_clock::time_point& time_point,
                          const unsigned int& device) {
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

std::string Buffer::Impl::GetFilepath(const std::chrono::system_clock::time_point& time_point,
                                      const unsigned int& device) {
    return filesystem_.GetFilepath(database_.FindHash(utility::SnapToMinute(time_point), device));
}

bool Buffer::Impl::Full() const {
    return filesystem_.AboveQuota();
}

bool Buffer::Impl::PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                                  const unsigned int& device) {
    try {
        database_.SetKeep(utility::SnapToMinute(time_point), device, PRESERVE_RECORD);
    } catch (const DatabaseException& e) {
        return false;
    }
    return true;
}

bool Buffer::Impl::Push(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device, const std::string& filepath) {
    while (filesystem_.AboveQuota()) {
        std::string hash;
        try {
            auto hash = database_.GetLowestDeletable();
        } catch (const DatabaseException& e) {
            return false;
        }

        if (hash.empty()) {
            fs::remove(filepath);
            return false;
        }

        filesystem_.Delete(hash);
        try {
            database_.Delete(hash);
        } catch (const DatabaseException& e) {
            return false;
        }
    }

    if (!fs::exists(filepath) || fs::is_directory(filepath)) {
        return false;
    }

    auto size = fs::file_size(filepath);
    auto hash = makeHash();

    if (filesystem_.Move(filepath, hash)) {
        try {
            database_.Insert(utility::SnapToMinute(time_point), device, hash, size, DELETE_IF_FULL);
        } catch (const DatabaseException& e) {
            filesystem_.Delete(hash);
        }
    } else {
        fs::remove(filepath);
    }
    return true;
}

std::string Buffer::Impl::makeHash(const int& len) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    std::stringstream stream;
    for (int i = 0; i < len; ++i) {
        stream << alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return stream.str();
}


// Bridge

Buffer::Buffer() : Buffer(std::string{}, 2.0) {}

Buffer::Buffer(const std::string& buffer_root) : Buffer(buffer_root, 2.0) {}

Buffer::Buffer(const std::string& buffer_root, const double& gigabyte_quota)
        : impl_{new Impl{buffer_root, gigabyte_quota}} {}

Buffer::~Buffer() {}

bool Buffer::Delete(const std::chrono::system_clock::time_point& time_point,
                    const unsigned int& device) {
    return impl_->Delete(time_point, device);
}

std::string Buffer::GetFilepath(const std::chrono::system_clock::time_point& time_point,
                                const unsigned int& device) {
    return impl_->GetFilepath(time_point, device);
}

bool Buffer::Full() const {
    return impl_->Full();
}

bool Buffer::PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device) {
    return impl_->PreserveRecord(time_point, device);
}

bool Buffer::Push(const std::chrono::system_clock::time_point& time_point,
                  const unsigned int& device, const std::string& filepath) {
    return impl_->Push(time_point, device, filepath);
}

} // namespace indexed
} // namespace prism