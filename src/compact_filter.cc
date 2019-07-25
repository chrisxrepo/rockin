#include "compact_filter.h"
#include <iostream>
#include <memory>
#include "cmd_interface.h"

namespace rockin {

MetaCompactFilter::MetaCompactFilter(const std::string& name) {
  name_ = name + "." + "MetaCompactFilter";
}

bool MetaCompactFilter::Filter(int level, const rocksdb::Slice& key,
                               const rocksdb::Slice& existing_value,
                               std::string* new_value,
                               bool* value_changed) const {
  if (existing_value.size() < BASE_META_VALUE_SIZE) {
    return true;
  }

  // check expire key
  uint64_t expire = META_VALUE_EXPIRE(existing_value.data());
  if (expire > 0 && GetMilliSec() >= expire) {
    char meta_buf[BASE_META_VALUE_SIZE];
    memset(meta_buf, 0, BASE_META_VALUE_SIZE);
    SET_META_VERSION(meta_buf, META_VALUE_VERSION(existing_value.data()) + 1);
    *new_value = std::string(meta_buf, BASE_META_VALUE_SIZE);
    *value_changed = true;
  }

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
DataCompactFilter::DataCompactFilter(const std::string& name, rocksdb::DB* db,
                                     rocksdb::ColumnFamilyHandle* handle)
    : db_(db), handle_(handle) {
  name_ = name + "." + "DataCompactFilter";
}

bool DataCompactFilter::Filter(int level, const rocksdb::Slice& key,
                               const rocksdb::Slice& existing_value,
                               std::string* new_value,
                               bool* value_changed) const {
  if (key.size() < BASE_FIELD_KEY_SIZE(1)) {
    return true;
  }

  size_t keylen = FIELD_KEY_LNE(key.data());
  const char* data = FIELD_KEY_START(key.data());
  uint32_t version = FIELD_KEY_VERSION(key.data());

  std::string meta;
  auto status = db_->Get(rocksdb::ReadOptions(), handle_,
                         rocksdb::Slice(data, keylen), &meta);

  // not found metadata
  if (status.IsNotFound()) return true;

  // metadata get error, return false
  if (!status.ok()) return false;

  // metadata size error
  if (meta.length() < BASE_META_VALUE_SIZE) return true;

  // check metadata type and version
  uint8_t type = META_VALUE_TYPE(meta.c_str());
  uint32_t meta_version = META_VALUE_VERSION(meta.c_str());
  if (type == Type_None || meta_version != version) {
    return true;
  }

  return false;
}

DataCompactFilterFactory::DataCompactFilterFactory(
    const std::string& name, rocksdb::DB** db,
    rocksdb::ColumnFamilyHandle** mt_handle)
    : name_(name), db_(db), mt_handle_(mt_handle) {}

std::unique_ptr<rocksdb::CompactionFilter>
DataCompactFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) {
  return std::unique_ptr<rocksdb::CompactionFilter>(
      new DataCompactFilter(name_, *db_, *mt_handle_));
}

}  // namespace rockin