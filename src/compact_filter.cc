#include "compact_filter.h"
#include <iostream>
#include <memory>

namespace rockin {

MetaCompactFilter::MetaCompactFilter(const std::string& name) {
  name_ = name + "." + "MetaCompactFilter";
}

bool MetaCompactFilter::Filter(int level, const rocksdb::Slice& key,
                               const rocksdb::Slice& existing_value,
                               std::string* new_value,
                               bool* value_changed) const {
  return false;
}

MetaCompactFilterFactory::MetaCompactFilterFactory(const std::string& name)
    : name_(name) {}

std::unique_ptr<rocksdb::CompactionFilter>
MetaCompactFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) {
  return std::unique_ptr<rocksdb::CompactionFilter>(
      new MetaCompactFilter(name_));
}

///////////////////////////////////////////////////
DataCompactFilter::DataCompactFilter(const std::string& name,
                                     rocksdb::ColumnFamilyHandle* handle)
    : handle_(handle) {
  name_ = name + "." + "DataCompactFilter";
}

bool DataCompactFilter::Filter(int level, const rocksdb::Slice& key,
                               const rocksdb::Slice& existing_value,
                               std::string* new_value,
                               bool* value_changed) const {
  return false;
}

DataCompactFilterFactory::DataCompactFilterFactory(
    const std::string& name,
    std::vector<rocksdb::ColumnFamilyHandle*>* mt_handls, int index)
    : name_(name), mt_handls_(mt_handls), index_(index) {}

std::unique_ptr<rocksdb::CompactionFilter>
DataCompactFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) {
  return std::unique_ptr<rocksdb::CompactionFilter>(
      new DataCompactFilter(name_, (*mt_handls_)[index_]));
}

}  // namespace rockin