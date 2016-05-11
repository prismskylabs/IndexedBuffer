#ifndef PRISM_INDEXED_DATABASE_H_
#define PRISM_INDEXED_DATABASE_H_

#include <exception>
#include <map>
#include <memory>
#include <string>
#include <vector>

#define DELETE_IF_FULL 0U
#define ATTEMPT_KEEP 10U
#define PRESERVE_RECORD 1000U


namespace prism {
namespace indexed {

using Record = std::map<std::string, std::string>;

class Database {
  public:
    Database(const std::string& path);
    ~Database();

    void Delete(const std::string& hash);
    void BulkDelete(const std::vector<std::string>& hash);
    std::vector<std::string> GetLowestDeletableHashes();
    std::string FindHash(const unsigned long long& time_value, const unsigned int& device);
    void Insert(const unsigned long long& time_value, const unsigned int& device,
                const std::string& hash, const unsigned long long& size, const unsigned int& keep);
    std::vector<Record> SelectAll();
    bool SetKeep(const unsigned long long& time_value, const unsigned int& device,
                 const unsigned int& keep);
    bool BulkSetKeep(const std::vector<unsigned long long>& time_values, const unsigned int& device,
                     const unsigned int& keep);

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
