#include <fstream>
#include <string>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include "database-fixture.h"


namespace fs = ::boost::filesystem;

class BufferFixture : public DatabaseFixture {
  protected:
    virtual void SetUp() {
        DatabaseFixture::SetUp();
        staging_path_ = fs::temp_directory_path() / fs::path{"prism_staging_buffer"};
        fs::create_directory(staging_path_);
        filename_ = "testfile";
        filepath_ = (staging_path_ / filename_).string();
        contents_ = "hello world";
    }

    virtual void TearDown() {
        DatabaseFixture::TearDown();
        fs::remove_all(staging_path_);
    }

    unsigned long writeStagingFile(const std::string& filename, const std::string& contents) {
        std::ofstream out_stream{(staging_path_ / filename).native()};
        out_stream << contents;
        return contents.length();
    }

    fs::path staging_path_;
    std::string filename_;
    std::string filepath_;
    std::string contents_;
};
