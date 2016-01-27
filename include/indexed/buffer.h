#ifndef PRISM_INDEXED_BUFFER_H
#define PRISM_INDEXED_BUFFER_H

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>


namespace prism {
namespace indexed {

struct Item {
    unsigned int minute;
};

using ItemMap = std::map<std::chrono::system_clock::time_point, std::vector<Item>>;
using Device = unsigned int;

class Buffer {
  public:
    Buffer();
    Buffer(const std::string& buffer_root);
    Buffer(const std::string& buffer_root, const double& gigabyte_quota);
    ~Buffer();

    bool Delete(const std::chrono::system_clock::time_point& time_point,
                const unsigned int& device);
    std::string GetBufferDirectory() const;
    std::map<Device, ItemMap> GetCatalog();
    std::string GetFilepath(const std::chrono::system_clock::time_point& time_point,
                            const unsigned int& device);
    bool Full() const;
    bool PreserveRecord(const std::chrono::system_clock::time_point& time_point,
                        const unsigned int& device);
    bool Push(const std::chrono::system_clock::time_point& time_point, const unsigned int& device,
              const std::string& filepath);

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace indexed
} // namespace prism

#endif /* PRISM_INDEXED_BUFFER_H_ */
