#ifndef PRISM_INDEXED_FILESYSTEM_H_
#define PRISM_INDEXED_FILESYSTEM_H_

#include <exception>
#include <memory>
#include <string>


namespace prism {
namespace indexed {

class Filesystem {
  public:
    Filesystem(const std::string& buffer_directory,
               const std::string& buffer_parent = std::string{},
               const double& gigabyte_quota = 2.0);
    ~Filesystem();

    bool AboveQuota();
    bool Delete(const std::string& filename);
    std::string GetBufferDirectory() const;
    std::string GetExistingFilepath(const std::string& filename) const;
    std::string GetFilepath(const std::string& filename) const;
    bool Move(const std::string& filepath_move_from, const std::string& filename_move_to);

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class FilesystemException : public std::exception {
  public:
    FilesystemException(const std::string& reason) : reason_(reason) {}
    virtual const char* what() const throw() {
        return reason_.data();
    }

  private:
    std::string reason_;
};

} // namespace indexed
} // namespace prism

#endif /* PRISM_INDEXED_FILESYSTEM_H_ */
