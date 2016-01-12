#include <gtest/gtest.h>

#include <chrono>

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
    prism::indexed::Buffer buffer(new_root.string());
    EXPECT_TRUE(fs::exists(new_root / "prism_indexed_buffer"));
}

TEST_F(BufferFixture, DifferentQuotaTest) {
    prism::indexed::Buffer buffer{std::string{}, 1.0};
}

TEST_F(BufferFixture, NegativeQuotaTest) {
    EXPECT_DEATH(prism::indexed::Buffer(std::string{}, -1.0), "");
}

TEST_F(BufferFixture, FullFalseTest) {
    prism::indexed::Buffer buffer;
    EXPECT_FALSE(buffer.Full());
}

TEST_F(BufferFixture, FullTrueTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    EXPECT_FALSE(buffer.Full());
    {
        std::ofstream out_stream{(buffer_path_ / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(buffer.Full());
}

TEST_F(BufferFixture, FullPushTrueTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    EXPECT_FALSE(buffer.Full());
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    buffer.Push(now, 1, filepath_);
    EXPECT_TRUE(buffer.Full());
}

TEST_F(BufferFixture, DeleteSingleFilesystemCheckTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    buffer.Push(now, 1, filepath_);
    EXPECT_EQ(1, numberOfFiles());
    EXPECT_TRUE(buffer.Delete(now, 1));
    EXPECT_EQ(0, numberOfFiles());
}

TEST_F(BufferFixture, DeleteFailSingleFilesystemCheckTest) {
    prism::indexed::Buffer buffer;
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(buffer.Delete(now, 1));
    EXPECT_EQ(0, numberOfFiles());
}

TEST_F(BufferFixture, DeleteSingleDatabaseCheckTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    buffer.Push(now, 1, filepath_);
    {
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    EXPECT_TRUE(buffer.Delete(now, 1));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(BufferFixture, DeleteSingleDatabaseCheckAfterFilesystemDeleteTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    buffer.Push(now, 1, filepath_);
    {
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(1, response.size());
    }
    auto hash = buffer.GetFilepath(now, 1);
    fs::remove(hash);
    EXPECT_EQ(0, numberOfFiles());
    EXPECT_TRUE(buffer.Delete(now, 1));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(BufferFixture, DeleteFailSingleDatabaseCheckTest) {
    prism::indexed::Buffer buffer;
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    {
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(0, response.size());
    }
    EXPECT_FALSE(buffer.Delete(now, 1));
    std::stringstream stream;
    stream << "SELECT * FROM "
           << table_name_
           << ";";
    auto response = execute(stream.str());
    EXPECT_EQ(0, response.size());
}

TEST_F(BufferFixture, DeleteFailSingleRemovedDatabaseCheckTest) {
    prism::indexed::Buffer buffer;
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    {
        std::stringstream stream;
        stream << "SELECT * FROM "
               << table_name_
               << ";";
        auto response = execute(stream.str());
        EXPECT_EQ(0, response.size());
    }
    fs::remove(db_path_);
    EXPECT_FALSE(buffer.Delete(now, 1));
}

TEST_F(BufferFixture, GetCatalogSingleTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(1, catalog.size());
    auto& item_map = catalog[1];
    EXPECT_EQ(1, item_map.size());
    for (auto& bucket : item_map) {
        EXPECT_EQ(1, bucket.second.size());
        EXPECT_LE(0, bucket.second[0].minute);
        EXPECT_GT(60, bucket.second[0].minute);
    }
}

TEST_F(BufferFixture, GetCatalogEmptyTest) {
    prism::indexed::Buffer buffer;
    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(0, catalog.size());
}

TEST_F(BufferFixture, GetCatalogRemovedDatabaseTest) {
    prism::indexed::Buffer buffer;
    fs::remove(db_path_);
    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(0, catalog.size());
}

TEST_F(BufferFixture, GetCatalogFullHourTest) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto hour_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count();
    std::chrono::system_clock::time_point hour{std::chrono::hours(hour_value)};

    for (int i = 0; i < 60; ++i) {
        std::chrono::system_clock::time_point tp{hour + std::chrono::minutes(i)};
        writeStagingFile(filename_, contents_);
        EXPECT_TRUE(buffer.Push(tp, 1, filepath_));
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(1, catalog.size());
    auto& item_map = catalog[1];
    EXPECT_EQ(1, item_map.size());
    for (auto& bucket : item_map) {
        EXPECT_EQ(60, bucket.second.size());
        auto last_minute = 0u;
        for (auto& item : bucket.second) {
            if (item.minute == 0u) {
                continue;
            }
            EXPECT_LT(last_minute, item.minute);
            last_minute = item.minute;
        }
    }
}

TEST_F(BufferFixture, GetCatalogTwoFullHourTest) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto hour_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count();
    std::chrono::system_clock::time_point hour{std::chrono::hours(hour_value)};

    for (int i = 0; i < 120; ++i) {
        std::chrono::system_clock::time_point tp{hour + std::chrono::minutes(i)};
        writeStagingFile(filename_, contents_);
        EXPECT_TRUE(buffer.Push(tp, 1, filepath_));
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(1, catalog.size());
    auto& item_map = catalog[1];
    EXPECT_EQ(2, item_map.size());
    for (auto& bucket : item_map) {
        EXPECT_EQ(60, bucket.second.size());
        auto last_minute = 0u;
        for (auto& item : bucket.second) {
            if (item.minute == 0u) {
                continue;
            }
            EXPECT_LT(last_minute, item.minute);
            last_minute = item.minute;
        }
    }
}

TEST_F(BufferFixture, GetCatalogFullDay) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto day_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count() / 24;
    std::chrono::system_clock::time_point day{std::chrono::hours(day_value * 24)};

    for (int i = 0; i < 24; ++i) {
        std::chrono::system_clock::time_point tp{day + std::chrono::hours(i)};
        writeStagingFile(filename_, contents_);
        EXPECT_TRUE(buffer.Push(tp, 1, filepath_));
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(1, catalog.size());
    auto& item_map = catalog[1];
    EXPECT_EQ(24, item_map.size());
    for (auto& bucket : item_map) {
        EXPECT_EQ(1, bucket.second.size());
        EXPECT_LE(0, bucket.second[0].minute);
        EXPECT_GT(60, bucket.second[0].minute);
    }
}

TEST_F(BufferFixture, GetCatalogMultipleDeviceTest) {
    prism::indexed::Buffer buffer;
    writeStagingFile(filename_, contents_);
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    writeStagingFile(filename_, contents_);
    EXPECT_TRUE(buffer.Push(now, 2, filepath_));
    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(2, catalog.size());
    for (auto& item_map : catalog) {
        EXPECT_EQ(1, item_map.second.size());
        for (auto& bucket : item_map.second) {
            EXPECT_EQ(1, bucket.second.size());
            EXPECT_LE(0, bucket.second[0].minute);
            EXPECT_GT(60, bucket.second[0].minute);
        }
    }
}

TEST_F(BufferFixture, GetCatalogFullHourMultipleDeviceTest) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto hour_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count();
    std::chrono::system_clock::time_point hour{std::chrono::hours(hour_value)};

    for (int i = 0; i < 60; ++i) {
        for (int j = 0; j < 10; ++j) {
            std::chrono::system_clock::time_point tp{hour + std::chrono::minutes(i)};
            writeStagingFile(filename_, contents_);
            EXPECT_TRUE(buffer.Push(tp, j, filepath_));
        }
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(10, catalog.size());
    for (auto& item_map : catalog) {
        EXPECT_EQ(1, item_map.second.size());
        for (auto& bucket : item_map.second) {
            EXPECT_EQ(60, bucket.second.size());
            auto last_minute = 0u;
            for (auto& item : bucket.second) {
                if (item.minute == 0u) {
                    continue;
                }
                EXPECT_LT(last_minute, item.minute);
                last_minute = item.minute;
            }
        }
    }
}

TEST_F(BufferFixture, GetCatalogTwoFullHourMultipleDeviceTest) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto hour_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count();
    std::chrono::system_clock::time_point hour{std::chrono::hours(hour_value)};

    for (int i = 0; i < 120; ++i) {
        for (int j = 0; j < 10; ++j) {
            std::chrono::system_clock::time_point tp{hour + std::chrono::minutes(i)};
            writeStagingFile(filename_, contents_);
            EXPECT_TRUE(buffer.Push(tp, j, filepath_));
        }
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(10, catalog.size());
    for (auto& item_map : catalog) {
        EXPECT_EQ(2, item_map.second.size());
        for (auto& bucket : item_map.second) {
            EXPECT_EQ(60, bucket.second.size());
            auto last_minute = 0u;
            for (auto& item : bucket.second) {
                if (item.minute == 0u) {
                    continue;
                }
                EXPECT_LT(last_minute, item.minute);
                last_minute = item.minute;
            }
        }
    }
}

TEST_F(BufferFixture, GetCatalogCompletelyFullDay) {
    prism::indexed::Buffer buffer;
    auto now = std::chrono::system_clock::now();
    auto day_value =
            std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch()).count() / 24;
    std::chrono::system_clock::time_point day{std::chrono::hours(day_value * 24)};

    for (int i = 0; i < 24; ++i) {
        for (int j = 0; j < 60; ++j) {
            std::chrono::system_clock::time_point tp{day + std::chrono::hours(i) +
                                                     std::chrono::minutes(j)};
            writeStagingFile(filename_, contents_);
            EXPECT_TRUE(buffer.Push(tp, 1, filepath_));
        }
    }

    auto catalog = buffer.GetCatalog();
    EXPECT_EQ(1, catalog.size());
    auto& item_map = catalog[1];
    EXPECT_EQ(24, item_map.size());
    for (auto& bucket : item_map) {
        EXPECT_EQ(60, bucket.second.size());
        auto last_minute = 0u;
        for (auto& item : bucket.second) {
            if (item.minute == 0u) {
                continue;
            }
            EXPECT_LT(last_minute, item.minute);
            last_minute = item.minute;
        }
    }
}

TEST_F(BufferFixture, PreserveRecordTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
    EXPECT_TRUE(buffer.PreserveRecord(now, 1));
}

TEST_F(BufferFixture, PreserveRecordWorksTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_TRUE(buffer.PreserveRecord(now, 1));
    writeStagingFile(filename_, contents_);
    EXPECT_FALSE(buffer.Push(now, 2, filepath_));
    EXPECT_EQ(1, numberOfFiles());
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
    EXPECT_EQ(PRESERVE_RECORD, std::stoi(record["keep"]));
}

TEST_F(BufferFixture, PreserveRecordRemovedDatabaseTest) {
    prism::indexed::Database database{db_string_};
    prism::indexed::Buffer buffer{std::string{}, (fs::file_size(db_path_) + 5) / (1024 * 1024.)};
    writeStagingFile(filename_, contents_);
    EXPECT_EQ(0, numberOfFiles());
    auto now = std::chrono::system_clock::now();
    EXPECT_TRUE(buffer.Push(now, 1, filepath_));
    EXPECT_EQ(1, numberOfFiles());
    fs::remove(db_path_);
    EXPECT_FALSE(buffer.PreserveRecord(now, 1));
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
