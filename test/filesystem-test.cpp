#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

#include <boost/filesystem.hpp>

#include "filesystem-fixture.h"
#include "indexed/filesystem.h"


namespace fs = ::boost::filesystem;

TEST_F(FilesystemFixture, EmptyFSTest) {
    EXPECT_FALSE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, ConstructFSTest) {
    EXPECT_FALSE(fs::exists(buffer_path_));
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, ConstructFSNoDestructTest) {
    EXPECT_FALSE(fs::exists(buffer_path_));
    {
        prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, ConstructFSMultipleTest) {
    EXPECT_FALSE(fs::exists(buffer_path_));
    {
        prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    {
        prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
        EXPECT_TRUE(fs::exists(buffer_path_));
    }
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, ConstructThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Filesystem filesystem{""};
    } catch (const prism::indexed::FilesystemException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"Cannot initialize indexed Filesystem with an empty buffer path"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(FilesystemFixture, ConstructCurrentThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Filesystem filesystem{"."};
    } catch (const prism::indexed::FilesystemException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"Filesystem must be initialized within a valid parent directory"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(FilesystemFixture, ConstructParentThrowTest) {
    bool thrown = false;
    try {
        prism::indexed::Filesystem filesystem{".."};
    } catch (const prism::indexed::FilesystemException& e) {
        thrown = true;
        EXPECT_EQ(std::string{"Filesystem must be initialized within a valid parent directory"},
                  std::string{e.what()});
    }
    EXPECT_TRUE(thrown);
}

TEST_F(FilesystemFixture, AboveQuotaFalseTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    EXPECT_FALSE(filesystem.AboveQuota());
}

TEST_F(FilesystemFixture, AboveQuotaTrueTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer", std::string{}, 5 / (1024 * 1024 * 1024.)};
    EXPECT_FALSE(filesystem.AboveQuota());
    {
        std::ofstream out_stream{(buffer_path_ / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(filesystem.AboveQuota());
}

TEST_F(FilesystemFixture, AboveQuotaNestedTrueTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer", std::string{}, 5 / (1024 * 1024 * 1024.)};
    EXPECT_FALSE(filesystem.AboveQuota());
    auto directory = buffer_path_ / "nested";
    fs::create_directory(directory);
    {
        std::ofstream out_stream{(directory / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(filesystem.AboveQuota());
}

TEST_F(FilesystemFixture, DeleteFalseTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    EXPECT_FALSE(fs::exists(buffer_path_ / "file"));
    EXPECT_FALSE(filesystem.Delete("file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "file"));
}

TEST_F(FilesystemFixture, DeleteFalseNullFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    EXPECT_FALSE(filesystem.Delete(""));
    EXPECT_TRUE(fs::exists(buffer_path_ / ""));
}

TEST_F(FilesystemFixture, DeleteFalseRelativeTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    EXPECT_FALSE(filesystem.Delete(".."));
    EXPECT_TRUE(fs::exists(buffer_path_ / ".."));
}

TEST_F(FilesystemFixture, DeleteTrueTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    {
        std::ofstream out_stream{(buffer_path_ / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(buffer_path_ / "file"));
    EXPECT_TRUE(filesystem.Delete("file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "file"));
}

TEST_F(FilesystemFixture, DeleteTrueRelativeTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    {
        std::ofstream out_stream{(buffer_path_ / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(buffer_path_ / "../prism_indexed_buffer/file"));
    EXPECT_TRUE(filesystem.Delete("../prism_indexed_buffer/file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "../prism_indexed_buffer/file"));
}

TEST_F(FilesystemFixture, DeleteRecursiveTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto directory = buffer_path_ / "nested";
    fs::create_directory(directory);
    {
        std::ofstream out_stream{(directory / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(buffer_path_ / "nested/file"));
    EXPECT_TRUE(filesystem.Delete("nested/file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "nested/file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "nested"));
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, DeleteDeeperRecursiveTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto directory = buffer_path_ / "nested" / "deeper";
    fs::create_directories(directory);
    {
        std::ofstream out_stream{(directory / "file").native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(buffer_path_ / "nested/deeper/file"));
    EXPECT_TRUE(filesystem.Delete("nested/deeper/file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "nested/deeper/file"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "nested/deeper"));
    EXPECT_FALSE(fs::exists(buffer_path_ / "nested"));
    EXPECT_TRUE(fs::exists(buffer_path_));
}

TEST_F(FilesystemFixture, GetBufferDirectoryTest) {
    const prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto buffer_directory = filesystem.GetBufferDirectory();
    EXPECT_EQ(fs::path{buffer_directory}, buffer_path_);
}

TEST_F(FilesystemFixture, GetExistingFilepathForNonExistingFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto path_string = filesystem.GetExistingFilepath("non_existing_file");
    EXPECT_TRUE(path_string.empty());
}

TEST_F(FilesystemFixture, GetExistingFilepathForExistingFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    {
        std::ofstream out_stream{(buffer_path_ / "existing_file").native()};
        out_stream << "hello world";
    }
    auto path_string = filesystem.GetExistingFilepath("existing_file");
    EXPECT_EQ(fs::path{path_string}, buffer_path_ / "existing_file");
}

TEST_F(FilesystemFixture, GetFilepathTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto path_string = filesystem.GetFilepath("file");
    EXPECT_EQ(fs::path{path_string}, buffer_path_ / "file");
}

TEST_F(FilesystemFixture, GetFilepathEmptyTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto path_string = filesystem.GetFilepath("");
    EXPECT_EQ(fs::path{path_string}, buffer_path_);
}

TEST_F(FilesystemFixture, GetFilepathCurrentTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto path_string = filesystem.GetFilepath(".");
    EXPECT_EQ(fs::path{path_string}, buffer_path_ / ".");
}

TEST_F(FilesystemFixture, MoveFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto filepath_move_from = buffer_path_ / "file";
    auto filepath_move_to = buffer_path_ / "file2";
    {
        std::ofstream out_stream{filepath_move_from.native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(filepath_move_from));
    EXPECT_FALSE(fs::exists(filepath_move_to));
    EXPECT_TRUE(filesystem.Move(filepath_move_from.string(), "file2"));
    EXPECT_FALSE(fs::exists(filepath_move_from));
    EXPECT_TRUE(fs::exists(filepath_move_to));
    {
        std::ifstream in_stream{filepath_move_to.native()};
        std::string in;
        std::getline(in_stream, in);
        EXPECT_EQ(std::string("hello world"), in);
    }
}

TEST_F(FilesystemFixture, MoveFileExistsTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto filepath_move_from = buffer_path_ / "file";
    auto filepath_move_to = buffer_path_ / "file2";
    {
        std::ofstream out_stream{filepath_move_from.native()};
        out_stream << "hello world";
    }
    {
        std::ofstream out_stream{filepath_move_to.native()};
        out_stream << "hello world!";
    }
    EXPECT_TRUE(fs::exists(filepath_move_from));
    EXPECT_TRUE(fs::exists(filepath_move_to));
    EXPECT_FALSE(filesystem.Move(filepath_move_from.string(), "file2"));
    EXPECT_TRUE(fs::exists(filepath_move_from));
    EXPECT_TRUE(fs::exists(filepath_move_to));
    {
        std::ifstream in_stream{filepath_move_to.native()};
        std::string in;
        std::getline(in_stream, in);
        EXPECT_EQ(std::string("hello world!"), in);
    }
}

TEST_F(FilesystemFixture, MoveFileDirectoryExistsTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto filepath_move_from = buffer_path_ / "file";
    auto directory = buffer_path_ / "file2";
    {
        std::ofstream out_stream{filepath_move_from.native()};
        out_stream << "hello world";
    }
    EXPECT_TRUE(fs::exists(filepath_move_from));
    fs::create_directory(directory);
    EXPECT_TRUE(fs::is_directory(directory));
    EXPECT_FALSE(filesystem.Move(filepath_move_from.string(), "file2"));
    EXPECT_TRUE(fs::exists(filepath_move_from));
    EXPECT_TRUE(fs::is_directory(directory));
}

TEST_F(FilesystemFixture, MoveNonexistentFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto filepath_move_from = buffer_path_ / "file";
    auto filepath_move_to = buffer_path_ / "file2";
    EXPECT_FALSE(fs::exists(filepath_move_from));
    EXPECT_FALSE(fs::exists(filepath_move_to));
    EXPECT_FALSE(filesystem.Move(filepath_move_from.string(), "file2"));
    EXPECT_FALSE(fs::exists(filepath_move_from));
    EXPECT_FALSE(fs::exists(filepath_move_to));
}
