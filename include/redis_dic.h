#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "siphash.h"
#include "utils.h"

#define DicUseRatio 0.2f

namespace rockin {

template <typename Node>
class RedisDic {
  // table
  struct DicTable {
    uint32_t size;
    uint32_t sizemask;
    uint32_t used;
    std::shared_ptr<Node>* head;
  };

 public:
  RedisDic() : rehashidx_(-1) {
    for (int i = 0; i < 2; i++) {
      table_[i] = (DicTable*)malloc(sizeof(DicTable));
      table_[i]->size = 0;
      table_[i]->used = 0;
      table_[i]->sizemask = 0;
      table_[i]->head = nullptr;
    }

    unsigned char seed[16];
    RandomBytes(seed, 16);
    hash_ = new SipHash(seed);
  }

  std::shared_ptr<Node> Get(const std::string& key) {
    if (table_[0]->used + table_[1]->used == 0) {
      return nullptr;
    }

    uint64_t h = hash_->Hash((unsigned char*)key.c_str(), key.length());
    for (int i = 0; i < 2; i++) {
      if (table_[i]->size == 0) continue;

      uint64_t idx = h & table_[i]->sizemask;
      auto node = table_[i]->head[idx];
      while (node) {
        if (node->key_ == key) return node;
        node = node->next_;
      }
    }
    return nullptr;
  }

  bool Delete(const std::string& key) {
    if (table_[0]->used + table_[1]->used == 0) {
      return false;
    }

    uint64_t h = hash_->Hash((unsigned char*)key.c_str(), key.length());
    for (int i = 0; i < 2; i++) {
      if (table_[i]->size == 0) continue;

      uint64_t idx = h & table_[i]->sizemask;
      auto node = table_[i]->head[idx];
      std::shared_ptr<Node> prev = nullptr;
      while (node) {
        if (node->key_ == key) {
          if (prev == nullptr) {
            table_[i]->head[idx] = node->next_;
          } else {
            prev->next_ = node->next_;
          }
          node->next_ = nullptr;
          return true;
        }
        prev = node;
        node = node->next_;
      }
    }
    return false;
  }

  size_t Size() { return table_[0]->used + table_[1]->used; }

  void Insert(std::shared_ptr<Node> node) {
    this->Expand();

    uint64_t idx =
        hash_->Hash((unsigned char*)node->key_.c_str(), node->key_.length()) &
        table_[0]->sizemask;

    node->next_ = table_[0]->head[idx];
    table_[0]->head[idx] = node;
    table_[0]->used++;
  }

  void Expand() {
    if (table_[1]->size > 0 ||
        (table_[0]->size > 0 &&
         float(table_[0]->used) / float(table_[0]->size) < DicUseRatio)) {
      return;
    }

    size_t resize = table_[0]->size * 2;
    if (resize < 4) resize = 4;

    resize = NextPower(resize);
    *table_[1] = *table_[0];

    table_[0]->size = resize;
    table_[0]->sizemask = resize - 1;
    table_[0]->used = 0;
    table_[0]->head = new std::shared_ptr<Node>[resize];
    if (table_[1]->used > 0) rehashidx_ = 0;
  }

  void RehashStep() {
    if (rehashidx_ != -1) Rehash(4);
  }

  void Rehash(int n) {
    if (table_[1]->size == 0) {
      return;
    }

    while (n > 0 && rehashidx_ < table_[1]->size) {
      Node* node = table_[1]->head[rehashidx_];
      while (node) {
        Node* next_ = node->next_;
        Insert(node);
        table_[1]->used--;
        node = next_;
      }
      table_[1]->head[rehashidx_] = nullptr;
      rehashidx_++;
      n--;
    }

    if (table_[1]->used == 0) {
      table_[1]->used = 0;
      table_[1]->size = 0;
      table_[1]->sizemask = 0;
      free(table_[1]->head);
      table_[1]->head = 0;
      rehashidx_ = -1;
    }
  }

 private:
  SipHash* hash_;
  DicTable* table_[2];
  int32_t rehashidx_;
};
}  // namespace rockin
