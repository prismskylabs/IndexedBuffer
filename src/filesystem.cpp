#include "indexed/filesystem.h"

#include <string>

#include <boost/filesystem.hpp>


namespace prism {
namespace indexed {

namespace fs = ::boost::filesystem;

class Filesystem::Impl {
  public:
    Impl(const std::string& buffer_directory, const std::string& buffer_parent);

    bool Delete(const std::string& filename);

  private:
    fs::path buffer_path_;
};

Filesystem::Impl::Impl(const std::string& buffer_directory, const std::string& buffer_parent) {
    auto parent_path = buffer_parent.empty() ? fs::temp_directory_path() : fs::path{buffer_parent};
    if (buffer_directory.empty()) {
        throw FilesystemException{"Cannot initialize indexed Filesystem with an empty buffer path"};
    }
    buffer_path_ = parent_path / fs::path{buffer_directory};
    if (fs::equivalent(buffer_path_, parent_path) ||
            fs::equivalent(buffer_path_, parent_path / fs::path{".."})) {
        throw FilesystemException{"Filesystem must be initialized within a valid parent directory"};
    }
    fs::create_directory(buffer_path_);
}

bool Filesystem::Impl::Delete(const std::string& filename) {
    auto filepath = buffer_path_ / fs::path{filename};
    if (!fs::is_directory(filepath)) {
        return fs::remove(filepath);
    }
    return false;
}


// Bridge

Filesystem::Filesystem(const std::string& buffer_directory, const std::string& buffer_parent)
        : impl_{new Impl{buffer_directory, buffer_parent}} {}
Filesystem::~Filesystem() {}

bool Filesystem::Delete(const std::string& filename) {
    return impl_->Delete(filename);
}

} // namespace indexed
} // namespace prism
