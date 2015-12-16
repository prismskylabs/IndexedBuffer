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
