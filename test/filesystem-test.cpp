#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

#include <boost/filesystem.hpp>

#include "filesystem-fixture.h"
#include "indexed/filesystem.h"


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
    EXPECT_TRUE(filesystem.Move(filepath_move_from.native(), "file2"));
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
    EXPECT_FALSE(filesystem.Move(filepath_move_from.native(), "file2"));
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
    EXPECT_FALSE(filesystem.Move(filepath_move_from.native(), "file2"));
    EXPECT_TRUE(fs::exists(filepath_move_from));
    EXPECT_TRUE(fs::is_directory(directory));
}

TEST_F(FilesystemFixture, MoveNonexistantFileTest) {
    prism::indexed::Filesystem filesystem{"prism_indexed_buffer"};
    auto filepath_move_from = buffer_path_ / "file";
    auto filepath_move_to = buffer_path_ / "file2";
    EXPECT_FALSE(fs::exists(filepath_move_from));
    EXPECT_FALSE(fs::exists(filepath_move_to));
    EXPECT_FALSE(filesystem.Move(filepath_move_from.native(), "file2"));
    EXPECT_FALSE(fs::exists(filepath_move_from));
    EXPECT_FALSE(fs::exists(filepath_move_to));
}
