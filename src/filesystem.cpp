#include "indexed/filesystem.h"

#include <chrono>
#include <string>

#include <boost/filesystem.hpp>


namespace prism {
namespace indexed {

namespace fs = ::boost::filesystem;

class Filesystem::Impl {
  public:
    Impl(const std::string& buffer_directory, const std::string& buffer_parent,
         const double& gigabyte_quota);

    bool AboveQuota();
    bool Delete(const std::string& filename);
    std::string GetBufferDirectory() const;
    std::string GetExistingFilepath(const std::string& filename) const;
    std::string GetFilepath(const std::string& filename) const;
    bool Move(const std::string& filepath_move_from, const std::string& filename_move_to);

  private:
    uintmax_t getSize() const;

    fs::path buffer_path_;
    double byte_quota_;
    uintmax_t size_;
    std::chrono::system_clock::time_point last_size_update_;
};

Filesystem::Impl::Impl(const std::string& buffer_directory, const std::string& buffer_parent,
                       const double& gigabyte_quota)
        : byte_quota_(gigabyte_quota * 1024 * 1024 * 1024) {
    auto parent_path = buffer_parent.empty() ? fs::temp_directory_path() : fs::path{buffer_parent};
    if (buffer_directory.empty()) {
        throw FilesystemException{"Cannot initialize indexed Filesystem with an empty buffer path"};
    }
    buffer_path_ = parent_path / buffer_directory;
    if (fs::equivalent(buffer_path_, parent_path) ||
            fs::equivalent(buffer_path_, parent_path / "..")) {
        throw FilesystemException{"Filesystem must be initialized within a valid parent directory"};
    }
    fs::create_directory(buffer_path_);
    size_ = getSize();
    last_size_update_ = std::chrono::system_clock::now();
}

bool Filesystem::Impl::AboveQuota() {
    auto now = std::chrono::system_clock::now();
    if (now - last_size_update_ > std::chrono::minutes(10)) {
        size_ = getSize();
        last_size_update_ = now;
    }
    auto space_info = fs::space(buffer_path_);
    auto fraction_space_available = space_info.available / static_cast<double>(space_info.capacity);

    return size_ > byte_quota_ || fraction_space_available < 0.1;
}

bool Filesystem::Impl::Delete(const std::string& filename) {
    auto filepath = buffer_path_ / filename;

    if (fs::is_directory(filepath)) {
        return false;
    }

    if (!fs::exists(filepath)) {
        return false;
    }

    auto removed_size = fs::file_size(filepath);
    auto success = fs::remove(filepath);
    if (!success) {
        return false;
    }
    size_ -= removed_size;

    auto parent_directory = fs::canonical(filepath.parent_path());
    const auto canonical_buffer_path = fs::canonical(buffer_path_);
    while (fs::is_empty(parent_directory) && parent_directory > canonical_buffer_path) {
        fs::remove(parent_directory);
        parent_directory = fs::canonical(parent_directory.parent_path());
    }

    return success;
}

std::string Filesystem::Impl::GetBufferDirectory() const {
    return buffer_path_.string();
}

std::string Filesystem::Impl::GetExistingFilepath(const std::string& filename) const {
    const auto filepath = buffer_path_ / filename;
    if (fs::exists(filepath)) {
        return filepath.string();
    }

    return std::string();
}

std::string Filesystem::Impl::GetFilepath(const std::string& filename) const {
    return (buffer_path_ / filename).string();
}

bool Filesystem::Impl::Move(const std::string& filepath_move_from,
                            const std::string& filename_move_to) {
    auto filepath = buffer_path_ / filename_move_to;
    if (fs::is_directory(filepath)) {
        return false;
    }
    const auto parent_directory = filepath.parent_path();
    if (!fs::exists(parent_directory)) {
        fs::create_directories(parent_directory);
    }
    if (fs::exists(filepath_move_from) && !fs::exists(filepath)) {
        try {
            fs::rename(filepath_move_from, filepath);
        } catch (const boost::filesystem::filesystem_error& e) {
            fs::copy_file(filepath_move_from, filepath);
            fs::remove(filepath_move_from);
        }
        size_ += fs::file_size(filepath);
        return true;
    }
    return false;
}

uintmax_t Filesystem::Impl::getSize() const {
    uintmax_t size = 0;
    const auto end = fs::recursive_directory_iterator();
    for (fs::recursive_directory_iterator it(buffer_path_); it != end;) {
        try {
            if (!fs::is_directory(*it)) {
                size += fs::file_size(*it);
            }
        } catch (const std::exception& e) {
        }

        // Increment to the next iterator
        try {
            ++it;
        } catch (const std::exception& e) {
            it.no_push();
            ++it;
        }
    }

    return size;
}


// Bridge

Filesystem::Filesystem(const std::string& buffer_directory, const std::string& buffer_parent,
                       const double& gigabyte_quota)
        : impl_{new Impl{buffer_directory, buffer_parent, gigabyte_quota}} {}

Filesystem::~Filesystem() {}

bool Filesystem::AboveQuota() {
    return impl_->AboveQuota();
}

bool Filesystem::Delete(const std::string& filename) {
    return impl_->Delete(filename);
}

std::string Filesystem::GetBufferDirectory() const {
    return impl_->GetBufferDirectory();
}

std::string Filesystem::GetExistingFilepath(const std::string& filename) const {
    return impl_->GetExistingFilepath(filename);
}

std::string Filesystem::GetFilepath(const std::string& filename) const {
    return impl_->GetFilepath(filename);
}

bool Filesystem::Move(const std::string& filepath_move_from, const std::string& filename_move_to) {
    return impl_->Move(filepath_move_from, filename_move_to);
}

} // namespace indexed
} // namespace prism
