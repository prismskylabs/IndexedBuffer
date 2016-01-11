#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include <boost/filesystem.hpp>

#include "database-fixture.h"
#include "indexed/database.h"


namespace fs = ::boost::filesystem;

TEST_F(DatabaseFixture, EmptyDBTest) {
    EXPECT_FALSE(fs::exists(db_path_));
}

TEST_F(DatabaseFixture, ConstructDBTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    prism::indexed::Database database{db_string_};
    EXPECT_TRUE(fs::exists(db_path_));
}

TEST_F(DatabaseFixture, ConstructDBNoDestructTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    {
        prism::indexed::Database database{db_string_};
        EXPECT_TRUE(fs::exists(db_path_));
    }
    EXPECT_TRUE(fs::exists(db_path_));
}

TEST_F(DatabaseFixture, ConstructDBMultipleTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    {
        prism::indexed::Database database{db_string_};
        EXPECT_TRUE(fs::exists(db_path_));
    }
    {
        prism::indexed::Database database{db_string_};
        EXPECT_TRUE(fs::exists(db_path_));
    }
    EXPECT_TRUE(fs::exists(db_path_));
}

TEST_F(DatabaseFixture, ConstructThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Database database{(fs::temp_directory_path() / "").string()};
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"unable to open database file"}, std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, ConstructCurrentThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Database database{(fs::temp_directory_path() / ".").string()};
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"unable to open database file"}, std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, ConstructParentThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Database database{(fs::temp_directory_path() / "..").string()};
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"unable to open database file"}, std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, InitialDBTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    prism::indexed::Database database{db_string_};
    EXPECT_TRUE(fs::exists(db_path_));
    std::stringstream stream;
    stream << "SELECT name FROM sqlite_master WHERE type='table' AND name='"
           << table_name_
           << "';";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(1, record.size());
    EXPECT_NE(record.end(), record.find("name"));
    EXPECT_EQ(std::string{"prism_indexed_data"}, record["name"]);
}

TEST_F(DatabaseFixture, InitialEmptyDBTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    prism::indexed::Database database{db_string_};
    EXPECT_TRUE(fs::exists(db_path_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(DatabaseFixture, InitialDBAfterDestructorTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    {
        prism::indexed::Database database{db_string_};
        EXPECT_TRUE(fs::exists(db_path_));
    }
    EXPECT_TRUE(fs::exists(db_path_));
    std::stringstream stream;
    stream << "SELECT name FROM sqlite_master WHERE type='table' AND name='"
           << table_name_
           << "';";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(1, record.size());
    EXPECT_NE(record.end(), record.find("name"));
    EXPECT_EQ(std::string{"prism_indexed_data"}, record["name"]);
}

TEST_F(DatabaseFixture, InitialEmptyDBAfterDestructorTest) {
    EXPECT_FALSE(fs::exists(db_path_));
    {
        prism::indexed::Database database{db_string_};
        EXPECT_TRUE(fs::exists(db_path_));
    }
    EXPECT_TRUE(fs::exists(db_path_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(DatabaseFixture, DeleteNullTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.Delete("");
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
}

TEST_F(DatabaseFixture, DeleteBadHashTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.Delete("h");
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
}

TEST_F(DatabaseFixture, DeleteSingleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.Delete("hash");
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(DatabaseFixture, DeleteCoupleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(2, 1, "hashbrowns", 10, 1);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(2, response.size());
    }
    database.Delete("hash");
    database.Delete("hashbrowns");
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(DatabaseFixture, DeleteManyTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 100;
    for (int i = 0; i < number_of_records; ++i) {
        database.Insert(i, 1, std::to_string(i * i), i * 2, i);
    }
    {
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(number_of_records, response.size());
    }
    for (int i = 0; i < number_of_records; ++i) {
        database.Delete(std::to_string(i * i));
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(number_of_records - i - 1, response.size());
    }
}

TEST_F(DatabaseFixture, FindHashEmptyTest) {
    prism::indexed::Database database{db_string_};
    EXPECT_TRUE(database.FindHash(1, 1).empty());
}

TEST_F(DatabaseFixture, FindHashTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    EXPECT_EQ(std::string{"hash"}, database.FindHash(1, 1));
}

TEST_F(DatabaseFixture, InsertEmptyHashTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "", 5, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(DatabaseFixture, InsertSingleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, InsertCoupleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(3, 1, "hashbrowns", 10, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    {
        auto& record = response[0];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(1, std::stoi(record["id"]));
        EXPECT_EQ(1, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hash"}, record["hash"]);
        EXPECT_EQ(5, std::stoi(record["size"]));
        EXPECT_EQ(0, std::stoi(record["keep"]));
    }
    {
        auto& record = response[1];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(2, std::stoi(record["id"]));
        EXPECT_EQ(3, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hashbrowns"}, record["hash"]);
        EXPECT_EQ(10, std::stoi(record["size"]));
        EXPECT_EQ(1, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, InsertCoupleUniqueTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(1, 2, "hashbrowns", 10, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    {
        auto& record = response[0];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(1, std::stoi(record["id"]));
        EXPECT_EQ(1, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hash"}, record["hash"]);
        EXPECT_EQ(5, std::stoi(record["size"]));
        EXPECT_EQ(0, std::stoi(record["keep"]));
    }
    {
        auto& record = response[1];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(2, std::stoi(record["id"]));
        EXPECT_EQ(1, std::stoi(record["time_value"]));
        EXPECT_EQ(2, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hashbrowns"}, record["hash"]);
        EXPECT_EQ(10, std::stoi(record["size"]));
        EXPECT_EQ(1, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, InsertCoupleUniquenessViolationTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    bool thrown = false;
    try {
        database.Insert(1, 1, "hashbrowns", 10, 1);
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(
                std::string{
                        "UNIQUE constraint failed: prism_indexed_data.time_value, "
                        "prism_indexed_data.device"},
                e.what());
    }
    EXPECT_TRUE(thrown);

    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, InsertManyTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 100;
    for (int i = 0; i < number_of_records; ++i) {
        database.Insert(i, 1, std::to_string(i * i), i * 2, i);
    }
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(number_of_records, response.size());
    for (int i = 0; i < number_of_records; ++i) {
        auto& record = response[i];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(i + 1, std::stoi(record["id"]));
        EXPECT_EQ(i, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::to_string(i * i), record["hash"]);
        EXPECT_EQ(i * 2, std::stoi(record["size"]));
        EXPECT_EQ(i, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, SelectAllTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    auto response = database.SelectAll();
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SelectAllManyTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 100;
    for (int i = 0; i < number_of_records; ++i) {
        database.Insert(i, 1, std::to_string(i * i), i * 2, i);
    }
    auto response = database.SelectAll();
    EXPECT_EQ(number_of_records, response.size());
    for (int i = 0; i < number_of_records; ++i) {
        auto& record = response[i];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(i + 1, std::stoi(record["id"]));
        EXPECT_EQ(i, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::to_string(i * i), record["hash"]);
        EXPECT_EQ(i * 2, std::stoi(record["size"]));
        EXPECT_EQ(i, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, SelectAllDeviceAscendingTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 100;
    for (int i = 0; i < number_of_records; ++i) {
        database.Insert(1, i, std::to_string(i * i), i * 2, i);
    }
    auto response = database.SelectAll();
    EXPECT_EQ(number_of_records, response.size());
    for (int i = 0; i < number_of_records; ++i) {
        auto& record = response[i];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(i + 1, std::stoi(record["id"]));
        EXPECT_EQ(1, std::stoi(record["time_value"]));
        EXPECT_EQ(i, std::stoi(record["device"]));
        EXPECT_EQ(std::to_string(i * i), record["hash"]);
        EXPECT_EQ(i * 2, std::stoi(record["size"]));
        EXPECT_EQ(i, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, SelectAllDevicePrecedenceTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 10;
    for (int i = 0; i < number_of_records; ++i) {
        for (int j = 0; j < number_of_records; ++j) {
            database.Insert(i, j, std::to_string(i * i), i * 2, i);
        }
    }
    auto response = database.SelectAll();
    EXPECT_EQ(number_of_records * number_of_records, response.size());
    for (int j = 0; j < number_of_records; ++j) {
        for (int i = 0; i < number_of_records; ++i) {
            auto& record = response[j * number_of_records + i];
            EXPECT_EQ(6, record.size());
            EXPECT_EQ(i, std::stoi(record["time_value"]));
            EXPECT_EQ(j, std::stoi(record["device"]));
            EXPECT_EQ(std::to_string(i * i), record["hash"]);
            EXPECT_EQ(i * 2, std::stoi(record["size"]));
            EXPECT_EQ(i, std::stoi(record["keep"]));
        }
    }
}

TEST_F(DatabaseFixture, SetKeepBadDeviceTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.SetKeep(1, 2, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SetKeepBadTimeTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.SetKeep(2, 1, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SetKeepSingleChangeTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.SetKeep(1, 1, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(1, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SetKeepSingleChangeReverseTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 1);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.SetKeep(1, 1, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SetKeepSingleNoChangeTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    database.SetKeep(1, 1, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_EQ(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_EQ(std::string{"hash"}, record["hash"]);
    EXPECT_EQ(5, std::stoi(record["size"]));
    EXPECT_EQ(0, std::stoi(record["keep"]));
}

TEST_F(DatabaseFixture, SetKeepCoupleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(3, 1, "hashbrowns", 10, 1);
    { 
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(2, response.size());
    }
    database.SetKeep(1, 1, 1);
    database.SetKeep(3, 1, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    {
        auto& record = response[0];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(1, std::stoi(record["id"]));
        EXPECT_EQ(1, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hash"}, record["hash"]);
        EXPECT_EQ(5, std::stoi(record["size"]));
        EXPECT_EQ(1, std::stoi(record["keep"]));
    }
    {
        auto& record = response[1];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(2, std::stoi(record["id"]));
        EXPECT_EQ(3, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::string{"hashbrowns"}, record["hash"]);
        EXPECT_EQ(10, std::stoi(record["size"]));
        EXPECT_EQ(0, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, SetKeepManyTest) {
    prism::indexed::Database database{db_string_};
    auto number_of_records = 100;
    for (int i = 0; i < number_of_records; ++i) {
        database.Insert(i, 1, std::to_string(i * i), i * 2, i);
        database.SetKeep(i, 1, i + 1);
    }
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(number_of_records, response.size());
    for (int i = 0; i < number_of_records; ++i) {
        auto& record = response[i];
        EXPECT_EQ(6, record.size());
        EXPECT_EQ(i + 1, std::stoi(record["id"]));
        EXPECT_EQ(i, std::stoi(record["time_value"]));
        EXPECT_EQ(1, std::stoi(record["device"]));
        EXPECT_EQ(std::to_string(i * i), record["hash"]);
        EXPECT_EQ(i * 2, std::stoi(record["size"]));
        EXPECT_EQ(i + 1, std::stoi(record["keep"]));
    }
}

TEST_F(DatabaseFixture, LowestDeletableNoneTest) {
    prism::indexed::Database database{db_string_};
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
    EXPECT_TRUE(database.GetLowestDeletable().empty());
}

TEST_F(DatabaseFixture, LowestDeletableSingleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    EXPECT_EQ(std::string{"hash"}, database.GetLowestDeletable());
}

TEST_F(DatabaseFixture, LowestDeletableCoupleTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(3, 1, "hashbrowns", 10, 1);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    EXPECT_EQ(std::string{"hash"}, database.GetLowestDeletable());
}

TEST_F(DatabaseFixture, LowestDeletableCoupleSwitchedTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 1);
    database.Insert(3, 1, "hashbrowns", 10, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    EXPECT_EQ(std::string{"hashbrowns"}, database.GetLowestDeletable());
}

TEST_F(DatabaseFixture, LowestDeletableCoupleEqualTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, 0);
    database.Insert(3, 1, "hashbrowns", 10, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    EXPECT_EQ(std::string{"hash"}, database.GetLowestDeletable());
}

TEST_F(DatabaseFixture, LowestDeletableCoupleEqualSwitchedTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(3, 1, "hash", 5, 0);
    database.Insert(1, 1, "hashbrowns", 10, 0);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(2, response.size());
    EXPECT_EQ(std::string{"hashbrowns"}, database.GetLowestDeletable());
}

TEST_F(DatabaseFixture, LowestDeletableTooHighTest) {
    prism::indexed::Database database{db_string_};
    database.Insert(1, 1, "hash", 5, PRESERVE_RECORD);
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    EXPECT_TRUE(database.GetLowestDeletable().empty());
}

TEST_F(DatabaseFixture, DeletedDBThrowInsertTest) {
    prism::indexed::Database database{db_string_};
    fs::remove(db_path_);
    EXPECT_FALSE(fs::exists(db_path_));
    bool thrown = false;
    try {
        database.Insert(1, 1, "hash", 5, 0);
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"no such table: prism_indexed_data"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, DeletedDBThrowFindHashTest) {
    prism::indexed::Database database{db_string_};
    fs::remove(db_path_);
    EXPECT_FALSE(fs::exists(db_path_));
    bool thrown = false;
    try {
        database.FindHash(1, 1);
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"no such table: prism_indexed_data"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, DeletedDBThrowDeleteTest) {
    prism::indexed::Database database{db_string_};
    fs::remove(db_path_);
    EXPECT_FALSE(fs::exists(db_path_));
    bool thrown = false;
    try {
        database.Delete("hash");
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"no such table: prism_indexed_data"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(DatabaseFixture, DeletedDBThrowSetKeepTest) {
    prism::indexed::Database database{db_string_};
    fs::remove(db_path_);
    EXPECT_FALSE(fs::exists(db_path_));
    bool thrown = false;
    try {
        database.SetKeep(1, 1, 1);
    } catch (const prism::indexed::DatabaseException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"no such table: prism_indexed_data"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}
