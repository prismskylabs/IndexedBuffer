#include "indexed/filesystem.h"

#include <string>

#include <boost/filesystem.hpp>


namespace prism {
namespace indexed {

namespace fs = ::boost::filesystem;

class Filesystem::Impl {
  public:
    Impl(const std::string& buffer_directory, const std::string& buffer_parent,
         const double& gigabyte_quota);

    bool AboveQuota() const;
    bool Delete(const std::string& filename);
    std::string GetFilepath(const std::string& filename) const;
    bool Move(const std::string& filepath_move_from, const std::string& filename_move_to);

  private:
    fs::path buffer_path_;
    double byte_quota_;
};

Filesystem::Impl::Impl(const std::string& buffer_directory, const std::string& buffer_parent,
                       const double& gigabyte_quota)
        : byte_quota_(gigabyte_quota * 1024 * 1024) {
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
}

bool Filesystem::Impl::AboveQuota() const {
    uintmax_t size = 0;
    for (fs::recursive_directory_iterator it(buffer_path_);
         it != fs::recursive_directory_iterator(); ++it) {
        if (!fs::is_directory(*it)) {
            size += fs::file_size(*it);
        }
    }

    auto fraction_space_available = fs::space(buffer_path_).available /
                                    static_cast<double>(fs::space(buffer_path_).capacity);

    return size > byte_quota_ || fraction_space_available < 0.1;
}

bool Filesystem::Impl::Delete(const std::string& filename) {
    auto filepath = buffer_path_ / filename;
    if (!fs::is_directory(filepath)) {
        return fs::remove(filepath);
    }
    return false;
}

std::string Filesystem::Impl::GetFilepath(const std::string& filename) const {
    return (buffer_path_ / filename).string();
}

bool Filesystem::Impl::Move(const std::string& filepath_move_from,
                            const std::string& filename_move_to) {
    auto filepath = buffer_path_ / filename_move_to;
    if (fs::exists(filepath_move_from) && !fs::exists(filepath)) {
        fs::rename(filepath_move_from, filepath);
        return true;
    }
    return false;
}


// Bridge

Filesystem::Filesystem(const std::string& buffer_directory, const std::string& buffer_parent,
                       const double& gigabyte_quota)
        : impl_{new Impl{buffer_directory, buffer_parent, gigabyte_quota}} {}

Filesystem::~Filesystem() {}

bool Filesystem::AboveQuota() const {
    return impl_->AboveQuota();
}

bool Filesystem::Delete(const std::string& filename) {
    return impl_->Delete(filename);
}

std::string Filesystem::GetFilepath(const std::string& filename) const {
    return impl_->GetFilepath(filename);
}

bool Filesystem::Move(const std::string& filepath_move_from, const std::string& filename_move_to) {
    return impl_->Move(filepath_move_from, filename_move_to);
}

} // namespace indexed
} // namespace prism
