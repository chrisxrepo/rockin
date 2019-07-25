#pragma once
#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>

namespace rockin {
class MetaCompactFilter : public rocksdb::CompactionFilter {
 public:
  MetaCompactFilter(const std::string& name);

  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override;

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
};

class MetaCompactFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  MetaCompactFilterFactory(const std::string& name);

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override;

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
};

//////////////////////////////////////////////////////////////////
class DataCompactFilter : public rocksdb::CompactionFilter {
 public:
  DataCompactFilter(const std::string& name, rocksdb::DB* db,
                    rocksdb::ColumnFamilyHandle* handle);

  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override;

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
  rocksdb::DB* db_;
  rocksdb::ColumnFamilyHandle* handle_;
};

class DataCompactFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  DataCompactFilterFactory(const std::string& name, rocksdb::DB** db,
                           rocksdb::ColumnFamilyHandle** mt_handle);

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override;

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
  rocksdb::DB** db_;
  rocksdb::ColumnFamilyHandle** mt_handle_;
};

}  // namespace rockin