#include <gtest/gtest.h>

#include <boost/filesystem.hpp>

#include "buffer-fixture.h"
#include "indexed/buffer.h"


namespace fs = ::boost::filesystem;

TEST_F(BufferFixture, ConstructDefaultTest) {
    prism::indexed::Buffer buffer;
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(BufferFixture, ConstructNoDestructTest) {
    {
        prism::indexed::Buffer buffer;
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(BufferFixture, ConstructMultipleTest) {
    {
        prism::indexed::Buffer buffer;
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    {
        prism::indexed::Buffer buffer;
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(BufferFixture, ConstructMultipleSameScopeTest) {
    prism::indexed::Buffer buffer;
    prism::indexed::Buffer another_buffer;
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(BufferFixture, ConstructDifferentRootTest) {
    auto new_root = buffer_path_ / "new_root";
    fs::create_directory(buffer_path_);
    EXPECT_FALSE(fs::exists(new_root));
    fs::create_directory(new_root);
    EXPECT_TRUE(fs::exists(new_root));
    prism::indexed::Buffer buffer(new_root.native());
    EXPECT_TRUE(fs::exists(new_root / "prism_indexed_buffer"));
}

TEST_F(BufferFixture, DifferentQuotaTest) {
    prism::indexed::Buffer buffer{std::string{}, 1.0};
}

TEST_F(BufferFixture, NegativeQuotaTest) {
    EXPECT_DEATH(prism::indexed::Buffer(std::string{}, -1.0), "");
}

TEST_F(BufferFixture, PushSingleFilesystemCheckTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
}

TEST_F(BufferFixture, PushNothingFilesystemCheckTest) {
    prism::indexed::Buffer buffer;
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(0, numberOfFiles());
}

TEST_F(BufferFixture, PushSingleDatabaseCheckTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
    auto& record = response[0];
    EXPECT_EQ(6, record.size());
    EXPECT_EQ(1, std::stoi(record["id"]));
    EXPECT_LE(1, std::stoi(record["time_value"]));
    EXPECT_EQ(1, std::stoi(record["device"]));
    EXPECT_FALSE(record["hash"].empty());
    EXPECT_EQ(DELETE_IF_FULL, std::stoi(record["keep"]));
}

TEST_F(BufferFixture, PushNothingDatabaseCheckTest) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(buffer.Push(now, 1, filepath_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(BufferFixture, PushAboveQuotaFilesystemCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(1), 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
}

TEST_F(BufferFixture, PushAlreadyAboveQuotaFilesystemCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) - 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(0, numberOfFiles());
}

TEST_F(BufferFixture, PushManyAboveQuotaFilesystemCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 40) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(1), 1, filepath_));
    EXPECT_EQ(2, numberOfFiles());
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(2), 1, filepath_));
    EXPECT_EQ(3, numberOfFiles());
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(3), 1, filepath_));
    EXPECT_EQ(4, numberOfFiles());
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(4), 1, filepath_));
    EXPECT_EQ(4, numberOfFiles());
}

TEST_F(BufferFixture, PushConstantlyAboveQuotaFilesystemCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
    for (auto i = 1; i < 100; ++i) {
        writeStagingFile(filename_, contents_);
        EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(i), 1, filepath_));
        EXPECT_EQ(1, numberOfFiles());
    }
}

TEST_F(BufferFixture, PushAboveQuotaDatabaseCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(1), 1, filepath_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
}

TEST_F(BufferFixture, PushAlreadyAboveQuotaDatabaseCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) - 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(buffer.Push(now, 1, filepath_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(BufferFixture, PushManyAboveQuotaDatabaseCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 40) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(1), 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(2), 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(3), 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(4), 1, filepath_));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(4, response.size());
}

TEST_F(BufferFixture, PushConstantlyAboveQuotaDatabaseCheckTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    for (auto i = 1; i < 100; ++i) {
        writeStagingFile(filename_, contents_);
        EXPECT_TRUE(buffer.Push(now + std::chrono::minutes(i), 1, filepath_));
    }
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(1, response.size());
}
