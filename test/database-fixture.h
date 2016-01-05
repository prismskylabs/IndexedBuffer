#include <map>
#include <string>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include "filesystem-fixture.h"
#include "indexed/database.h"


namespace fs = ::boost::filesystem;

class DatabaseFixture : public FilesystemFixture {
  protected:
    using Record = std::map<std::string, std::string>;

    virtual void SetUp() {
        FilesystemFixture::SetUp();
        fs::create_directory(buffer_path_);
        db_path_ = buffer_path_ / "prism_indexed_data.db";
        db_string_ = db_path_.string();
        table_name_ = "prism_indexed_data";
    }

    sqlite3* openDatabase() {
        sqlite3* sqlite_db;
        if (sqlite3_open(db_string_.data(), &sqlite_db) != SQLITE_OK) {
            throw prism::indexed::DatabaseException{sqlite3_errmsg(sqlite_db)};
        }
        return sqlite_db;
    }

    int closeDatabase(sqlite3* db) {
        return sqlite3_close(db);
    }

    static int callback(void* response_ptr, int num_values, char** values, char** names) {
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

    std::vector<Record> execute(const std::string& sql) {
        std::vector<Record> response;
        auto db = openDatabase();
        char* error;
        int rc = sqlite3_exec(db, sql.data(), &callback, &response, &error);
        if (rc != SQLITE_OK) {
            auto error_string = std::string{error};
            sqlite3_free(error);
            throw prism::indexed::DatabaseException{error_string};
        }

        return response;
    }

    fs::path db_path_;
    std::string db_string_;
    std::string table_name_;
};
