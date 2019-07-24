#include "disk_saver.h"
#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <mutex>

#include "compact_filter.h"
#include "rocksdb/filter_policy.h"
#include "siphash.h"
#include "utils.h"

namespace rockin {

namespace {
std::once_flag disk_saver_once_flag;
DiskSaver *g_disk_saver;
};  // namespace

DiskSaver *DiskSaver::Default() {
  std::call_once(disk_saver_once_flag,
                 []() { g_disk_saver = new DiskSaver(); });
  return g_disk_saver;
}

void DiskSaver::DestoryDefault() {
  if (g_disk_saver == nullptr) {
    delete g_disk_saver;
    g_disk_saver = nullptr;
  }
}

struct DiskDB {
  rocksdb::DB *db;
  rocksdb::ColumnFamilyHandle *mt_handle;
  rocksdb::ColumnFamilyHandle *db_handle;
  std::string partition_name;
  int partition_id;
};

DiskSaver::DiskSaver()
    : partition_num_(0),
      read_thread_num_(0),
      write_thread_num_(0),
      read_async_(100000),
      write_async_(nullptr) {}

DiskSaver::~DiskSaver() {
  LOG(INFO) << "destroy rocks pool...";
  // partitions_.clear();
}

bool DiskSaver::Init(int read_thread_num, int write_thread_num,
                     int partition_num, const std::string &path) {
  path_ = path;
  read_thread_num_ = read_thread_num;
  write_thread_num_ = write_thread_num;
  partition_num_ = partition_num;
  write_async_ = new AsyncQueue(partition_num, 10000);

  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);

  meta_cache_ = rocksdb::NewLRUCache(1 << 30);
  data_cache_ = rocksdb::NewLRUCache(128 << 20);

  for (int i = 0; i < partition_num; i++) {
    DiskDB *db = new DiskDB();
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::string partition_name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + partition_name;

    // meta
    rocksdb::ColumnFamilyOptions mt_cf_ops;
    rocksdb::BlockBasedTableOptions mt_table_ops;
    mt_table_ops.block_size = 4096;  // 4k
    mt_table_ops.partition_filters = true;
    mt_table_ops.index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    mt_table_ops.block_cache = meta_cache_;
    mt_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    mt_cf_ops.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(mt_table_ops));
    mt_cf_ops.compaction_filter_factory =
        std::make_shared<MetaCompactFilterFactory>(partition_name + ":mt");
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("mt", mt_cf_ops));

    // data
    rocksdb::ColumnFamilyOptions db_cf_ops;
    rocksdb::BlockBasedTableOptions db_table_ops;
    db_table_ops.block_size = 4096;  // 4k
    db_table_ops.block_cache = data_cache_;
    db_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    db_cf_ops.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(db_table_ops));
    db_cf_ops.compaction_filter_factory =
        std::make_shared<DataCompactFilterFactory>(partition_name + ":data",
                                                   &db->db, &db->mt_handle);
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor("data", db_cf_ops));

    // default
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

    rocksdb::Options ops;
    ops.env = rocksdb::Env::Default();
    ops.create_if_missing = true;
    ops.create_missing_column_families = true;
    ops.write_buffer_size = 256 << 20;        // 256M
    ops.target_file_size_base = 20 << 20;     // 20M
    ops.max_background_flushes = 1;           // flush后台线程数量
    ops.max_background_compactions = 2;       // compact线程数量
    ops.max_bytes_for_level_multiplier = 10;  // 每层SST文件大小因子
    ops.optimize_filters_for_hits = false;  // True:最大层STT文件没有filter
    ops.level_compaction_dynamic_level_bytes = false;
    ops.max_open_files = 5000;

    std::vector<rocksdb::ColumnFamilyHandle *> handles;

    // ops.write_buffer_manager
    auto status =
        rocksdb::DB::Open(ops, dbpath, column_families, &handles, &db->db);
    LOG_IF(FATAL, !status.ok())
        << "rocksdb::DB::Open status:" << status.getState();
    LOG_IF(FATAL, column_families.size() != 3)
        << "column family size:" << column_families.size();

    db->partition_id = i;
    db->partition_name = partition_name;
    db->mt_handle = handles[0];
    db->db_handle = handles[1];
    LOG(INFO) << "open rocksdb:" << partition_name;
    dbs_.push_back(db);
  }

  return this->InitAsync(read_thread_num + write_thread_num);
}

void DiskSaver::AsyncWork(int idx) {
  if (idx < read_thread_num_)
    this->ReadAsyncWork(idx);
  else
    this->WriteAsyncWork(idx - read_thread_num_);
}

void DiskSaver::ReadAsyncWork(int idx) {
  while (true) {
    QUEUE *q = read_async_.Pop();
    uv__work *w = QUEUE_DATA(q, struct uv__work, wq);
    w->work(w);

    // async done
    uv_mutex_lock(&w->loop->wq_mutex);
    w->work = NULL;
    QUEUE_INSERT_TAIL(&w->loop->wq, &w->wq);
    uv_async_send(&w->loop->wq_async);
    uv_mutex_unlock(&w->loop->wq_mutex);
  }
}

void DiskSaver::WriteAsyncWork(int idx) {
  while (true) {
    int queue_idx;
    std::vector<uv__work *> works;
    auto qs = write_async_->Pop(queue_idx, 100);
    for (auto iter = qs.begin(); iter != qs.end(); ++iter) {
      uv__work *w = QUEUE_DATA(*iter, struct uv__work, wq);
      works.push_back(w);
    }

    if (works.size() > 0) {
      this->WriteBatch(queue_idx, works);

      // async done
      for (size_t i = 0; i < works.size(); i++) {
        uv__work *w = works[i];
        uv_mutex_lock(&w->loop->wq_mutex);
        w->work = NULL;
        QUEUE_INSERT_TAIL(&w->loop->wq, &w->wq);
        uv_async_send(&w->loop->wq_async);
        uv_mutex_unlock(&w->loop->wq_mutex);
      }
    }
  }
}

void DiskSaver::PostWork(int idx, QUEUE *q) {
  if (idx < 0) {
    read_async_.Push(q);
  } else if (idx < partition_num_) {
    write_async_->Push(q, idx);
  } else {
    LOG(ERROR) << "PostWork invilid partition idx:" << idx;
  }
}

DiskDB *DiskSaver::GetDiskRocksdb(BufPtr key) {
  if (DiskSaver::Default()->partition_num_ == 1) return dbs_[0];
  int index = rockin::SimpleHash(key->data, key->len) % partition_num_;
  return dbs_[index];
}

//////////////////////////////////////////////////////////////////

static bool GetFromRocksdb(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle,
                           BufPtr key, std::string &value) {
  auto status = db->Get(rocksdb::ReadOptions(), handle,
                        rocksdb::Slice(key->data, key->len), &value);
  if (status.ok()) return true;
  if (!status.IsNotFound()) LOG(ERROR) << "rocksdb Get:" << status.ToString();
  return false;
}

static bool GetFromRocksdb(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle,
                           std::vector<BufPtr> &keys, std::vector<bool> &exists,
                           std::vector<std::string> &value) {
  exists.clear();
  std::vector<rocksdb::Slice> key_slices;
  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  for (size_t i = 0; i < keys.size(); i++) {
    handles.push_back(handle);
    key_slices.push_back(rocksdb::Slice(keys[i]->data, keys[i]->len));
  }

  auto statuss =
      db->MultiGet(rocksdb::ReadOptions(), handles, key_slices, &value);
  for (size_t i = 0; i < statuss.size(); i++) {
    if (statuss[i].ok())
      exists.push_back(true);
    else if (statuss[i].IsNotFound())
      exists.push_back(false);
    else {
      LOG(ERROR) << "rocksdb MuiltGet:" << statuss[i].ToString();
      return false;
    }
  }

  return true;
}

struct GetMetaHelper {
  BufPtr mkey;
  bool exist;
  std::string value;
  DiskSaver::MetaCB cb;
};

void DiskSaver::GetMeta(uv_loop_t *loop, BufPtr mkey, MetaCB cb) {
  if (loop == nullptr || mkey == nullptr || read_thread_num_ == 0) return;
  GetMetaHelper *helper = new GetMetaHelper();
  helper->mkey = mkey;
  helper->cb = cb;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  AsyncQueueWork(-1, loop, req,
                 [](uv_work_t *req) {
                   GetMetaHelper *helper = (GetMetaHelper *)req->data;
                   DiskDB *db =
                       DiskSaver::Default()->GetDiskRocksdb(helper->mkey);
                   helper->exist = GetFromRocksdb(db->db, db->mt_handle,
                                                  helper->mkey, helper->value);
                 },
                 [](uv_work_t *req, int status) {
                   GetMetaHelper *helper = (GetMetaHelper *)req->data;
                   helper->cb(helper->exist, helper->mkey, helper->value);
                   delete helper;
                   free(req);
                 });
}

struct GetArrayHelper {
  BufPtr mkey;
  BufPtrs keys;
  std::vector<bool> exists;
  std::vector<std::string> values;
  DiskSaver::ArrayValueCB cb;
};

void DiskSaver::GetValues(uv_loop_t *loop, BufPtr mkey, BufPtrs keys,
                          ArrayValueCB cb) {
  if (loop == nullptr || mkey == nullptr || read_thread_num_ == 0) return;
  GetArrayHelper *helper = new GetArrayHelper();
  helper->mkey = mkey;
  helper->keys = keys;
  helper->cb = cb;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  AsyncQueueWork(
      -1, loop, req,
      [](uv_work_t *req) {
        GetArrayHelper *helper = (GetArrayHelper *)req->data;
        DiskDB *db = DiskSaver::Default()->GetDiskRocksdb(helper->mkey);

        bool exist = GetFromRocksdb(db->db, db->db_handle, helper->keys,
                                    helper->exists, helper->values);
        if (!exist) {
          helper->exists.clear();
          helper->values.clear();
        }
      },
      [](uv_work_t *req, int status) {
        GetArrayHelper *helper = (GetArrayHelper *)req->data;
        helper->cb(helper->mkey, helper->exists, helper->values);
        delete helper;
        free(req);
      });
}

struct SetHelper {
  bool ok;
  BufPtr mkey;
  BufPtr meta;
  BufPtrs keys;
  BufPtrs values;
  DiskSaver::SetCB cb;
};

void DiskSaver::WriteBatch(int idx, const std::vector<uv__work *> &works) {
  if (works.size() == 0) return;
  DiskDB *db = dbs_[idx];

  rocksdb::WriteBatch batch;
  for (size_t i = 0; i < works.size(); i++) {
    uv_work_t *req = container_of(works[i], uv_work_t, work_req);
    SetHelper *helper = (SetHelper *)req->data;
    if (helper->meta != nullptr) {
      auto status = batch.Put(
          db->mt_handle, rocksdb::Slice(helper->mkey->data, helper->mkey->len),
          rocksdb::Slice(helper->meta->data, helper->meta->len));
      if (!status.ok()) {
        LOG(ERROR) << "WriteBatch Put:" << status.ToString();
        return;
      }
    }

    for (size_t j = 0; j < helper->keys.size() && j < helper->values.size();
         j++) {
      auto key = helper->keys[j];
      auto value = helper->values[j];
      auto status =
          batch.Put(db->db_handle, rocksdb::Slice(key->data, key->len),
                    rocksdb::Slice(value->data, value->len));
      if (!status.ok()) {
        LOG(ERROR) << "WriteBatch Put:" << status.ToString();
        return;
      }
    }
  }

  auto status = db->db->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    for (size_t i = 0; i < works.size(); i++) {
      uv_work_t *req = container_of(works[i], uv_work_t, work_req);
      SetHelper *helper = (SetHelper *)req->data;
      helper->ok = false;
    }

    LOG(ERROR) << "WriteBatch Write:" << status.ToString();
  }
}

void DiskSaver::Set(uv_loop_t *loop, BufPtr mkey, BufPtr meta, SetCB cb) {
  this->Set(loop, mkey, meta, BufPtrs(), BufPtrs(), cb);
}

void DiskSaver::Set(uv_loop_t *loop, BufPtr mkey, BufPtrs keys, BufPtrs values,
                    SetCB cb) {
  this->Set(loop, mkey, nullptr, keys, values, cb);
}

void DiskSaver::Set(uv_loop_t *loop, BufPtr mkey, BufPtr meta, BufPtrs keys,
                    BufPtrs values, SetCB cb) {
  if (loop == nullptr || mkey == nullptr || write_thread_num_ == 0) return;

  SetHelper *helper = new SetHelper();
  helper->mkey = mkey;
  helper->meta = meta;
  helper->keys = keys;
  helper->values = values;
  helper->cb = cb;
  helper->ok = true;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  int index = rockin::SimpleHash(mkey->data, mkey->len) % partition_num_;
  AsyncQueueWork(index, loop, req, nullptr, [](uv_work_t *req, int status) {
    SetHelper *helper = (SetHelper *)req->data;
    helper->cb(helper->ok);
    delete helper;
    free(req);
  });
}

void DiskSaver::Compact() {
  for (size_t i = 0; i < dbs_.size(); i++) {
    LOG(INFO) << "Start to compct rocksdb:" << dbs_[i]->partition_name;

    auto status = dbs_[i]->db->CompactRange(
        rocksdb::CompactRangeOptions(), dbs_[i]->mt_handle, nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Compact partition:" << dbs_[i]->partition_name
                 << ", metadata error:" << status.ToString();
    }

    status = dbs_[i]->db->CompactRange(rocksdb::CompactRangeOptions(),
                                       dbs_[i]->db_handle, nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Compact partition:" << dbs_[i]->partition_name
                 << ", dbdata error:" << status.ToString();
    }
  }
}

}  // namespace rockin