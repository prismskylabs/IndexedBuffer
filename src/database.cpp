#include "indexed/database.h"

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <sqlite3.h>


namespace prism {
namespace indexed {

class Database::Impl {
  public:
    Impl(const std::string& path);

    void Delete(const std::string& hash);
    std::string GetLowestDeletable();
    std::string FindHash(const unsigned long long& time_value, const unsigned int& device);
    void Insert(const unsigned long long& time_value, const unsigned int& device,
                const std::string& hash, const unsigned long long& size, const unsigned int& keep);
    std::vector<Record> SelectAll();
    void SetKeep(const unsigned long long& time_value, const unsigned int& device,
                 const unsigned int& keep);

  private:
    using DatabaseHandle = std::unique_ptr<sqlite3, std::function<int(sqlite3*)>>;

    static int callback(void* response_ptr, int num_values, char** values, char** names);

    bool checkTable();
    void createTable();
    std::vector<Record> execute(const std::string& sql);
    DatabaseHandle openDatabase();

    std::string table_path_;
    std::string table_name_;
};

Database::Impl::Impl(const std::string& path)
        : table_path_(path), table_name_("prism_indexed_data") {
    if (!checkTable()) {
        createTable();
    }
}

void Database::Impl::Delete(const std::string& hash) {
    if (hash.empty()) {
        return;
    }

    std::stringstream stream;
    stream << "DELETE FROM "
           << table_name_
           << " WHERE hash='"
           << hash
           << "';";
    execute(stream.str());
}

std::string Database::Impl::GetLowestDeletable() {
    std::stringstream stream;
    stream << "SELECT hash FROM "
           << table_name_
           << " WHERE keep < " << PRESERVE_RECORD
           << " ORDER BY keep ASC, time_value ASC LIMIT 1;";
    auto response = execute(stream.str());
    std::string hash;
    if (!response.empty()) {
        auto& record = response[0];
        if (!record.empty()) {
            hash = record["hash"];
        }
    }

    return hash;
}

std::string Database::Impl::FindHash(const unsigned long long& time_value,
                                     const unsigned int& device) {
    std::stringstream stream;
    stream << "SELECT hash FROM "
           << table_name_
           << " WHERE time_value=" << time_value
           << " AND device=" << device
           << ";";
    std::string hash;
    auto response = execute(stream.str());
    if (!response.empty()) {
        auto& record = response[0];
        if (!record.empty()) {
            hash = record["hash"];
        }
    }
    return hash;
}

void Database::Impl::Insert(const unsigned long long& time_value, const unsigned int& device,
                            const std::string& hash, const unsigned long long& size,
                            const unsigned int& keep) {
    if (hash.empty()) {
        return;
    }

    std::stringstream stream;
    stream << "INSERT INTO "
           << table_name_
           << "(time_value, device, hash, size, keep)"
           << "VALUES"
           << "("
           << time_value << ","
           << device << ","
           << "'" << hash << "',"
           << size << ","
           << keep
           << ");";
    execute(stream.str());
}

std::vector<Record> Database::Impl::SelectAll() {
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << " ORDER BY device ASC, time_value ASC;";
    return execute(stream.str());
}

void Database::Impl::SetKeep(const unsigned long long& time_value, const unsigned int& device,
                             const unsigned int& keep) {
    std::stringstream stream;
    stream << "UPDATE "
           << table_name_
           << " SET keep="
           << keep
           << " WHERE time_value=" << time_value
           << " AND device=" << device
           << ";";
    execute(stream.str());
}

int Database::Impl::callback(void* response_ptr, int num_values, char** values, char** names) {
    auto response = (std::vector<Record>*) response_ptr;
    auto record = Record();
    for (int i = 0; i < num_values; ++i) {
        if (values[i]) {
            record[names[i]] = values[i];
        }
    }

    response->push_back(record);

    return 0;
}

bool Database::Impl::checkTable() {
    std::stringstream stream;
    stream << "SELECT name FROM sqlite_master WHERE type='table' AND name='"
           << table_name_
           << "';";
    auto response = execute(stream.str());

    return !response.empty();
}

void Database::Impl::createTable() {
    std::stringstream stream;
    stream << "CREATE TABLE "
           << table_name_
           << "("
           << "id INTEGER PRIMARY KEY AUTOINCREMENT,"
           << "time_value UNSIGNED BIGINT NOT NULL,"
           << "device UNSIGNED INT NOT NULL,"
           << "hash TEXT NOT NULL,"
           << "size UNSIGNED BIGINT NOT NULL,"
           << "keep UNSIGNED INT NOT NULL,"
           << "UNIQUE (time_value, device) ON CONFLICT ROLLBACK"
           << ");";
    execute(stream.str());
}

std::vector<Record> Database::Impl::execute(const std::string& sql_statement) {
    std::vector<Record> response;
    auto sqlite_database = openDatabase();
    char* error;
    int rc = sqlite3_exec(sqlite_database.get(), sql_statement.data(), &Database::Impl::callback,
                          &response, &error);
    if (rc != SQLITE_OK) {
        auto error_string = std::string{error};
        sqlite3_free(error);
        throw DatabaseException{error_string};
    }

    return response;
}

Database::Impl::DatabaseHandle Database::Impl::openDatabase() {
    sqlite3* sqlite_db;
    if (sqlite3_open(table_path_.data(), &sqlite_db) != SQLITE_OK) {
        throw DatabaseException{sqlite3_errmsg(sqlite_db)};
    }
    return DatabaseHandle(sqlite_db, sqlite3_close);
}


// Bridge

Database::Database(const std::string& path) : impl_{new Impl{path}} {}

Database::~Database() {}

void Database::Delete(const std::string& hash) {
    impl_->Delete(hash);
}

std::string Database::GetLowestDeletable() {
    return impl_->GetLowestDeletable();
}

std::string Database::FindHash(const unsigned long long& time_value, const unsigned int& device) {
    return impl_->FindHash(time_value, device);
}

void Database::Insert(const unsigned long long& time_value, const unsigned int& device,
                      const std::string& hash, const unsigned long long& size,
                      const unsigned int& keep) {
    impl_->Insert(time_value, device, hash, size, keep);
}

std::vector<Record> Database::SelectAll() {
    return impl_->SelectAll();
}

void Database::SetKeep(const unsigned long long& time_value, const unsigned int& device,
                       const unsigned int& keep) {
    impl_->SetKeep(time_value, device, keep);
}

} // namespace indexed
} // namespace prism
