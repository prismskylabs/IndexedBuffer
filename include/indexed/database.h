#ifndef PRISM_INDEXED_DATABASE_H_
#define PRISM_INDEXED_DATABASE_H_

#include <exception>
#include <memory>
#include <string>

#define PRESERVE_RECORD 1000U


namespace prism {
namespace indexed {

class Database {
  public:
    Database(const std::string& path);
    ~Database();

    void Delete(const std::string& hash);
    std::string GetLowestDeletable();
    void Insert(const unsigned long long& time_value, const unsigned int& device,
                const std::string& hash, const unsigned long long& size, const unsigned int& keep);
    void SetKeep(const std::string& hash, const unsigned int& keep);

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class DatabaseException : public std::exception {
  public:
    DatabaseException(const std::string& reason) : reason_(reason) {}
    virtual const char* what() const throw() {
        return reason_.data();
    }

  private:
    std::string reason_;
};

} // namespace indexed
} // namespace prism

#endif /* PRISM_INDEXED_DATABASE_H_ */
