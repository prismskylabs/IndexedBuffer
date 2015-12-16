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
