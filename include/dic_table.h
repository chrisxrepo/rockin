#pragma once
#include <stdint.h>
#include <string.h>
#include <functional>
#include <iostream>
#include "rockin_alloc.h"
#include "siphash.h"
#include "utils.h"

#define DicUseRatio 0.2f

namespace rockin {

template <typename T>
class DicTable {
 public:
  struct Node {
    std::shared_ptr<T> data;
    Node* next;
  };

  struct Table {
    uint32_t size;
    uint32_t sizemask;
    uint32_t used;
    Node** head;
  };

  DicTable(std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)> equal,
           std::function<uint64_t(std::shared_ptr<T>)> hash)
      : rehashidx_(-1), equal_(equal), hash_(hash) {
    for (int i = 0; i < 2; i++) {
      table_[i] = (Table*)malloc(sizeof(Table));
      table_[i]->size = 0;
      table_[i]->used = 0;
      table_[i]->sizemask = 0;
      table_[i]->head = nullptr;
    }
  }

  Node* Get(std::shared_ptr<T> key) {
    if (table_[0]->used + table_[1]->used == 0) {
      return nullptr;
    }
    RehashStep();

    uint64_t h = hash_(key);
    for (int i = 0; i < 2; i++) {
      if (table_[i]->size == 0) continue;

      uint64_t idx = h & table_[i]->sizemask;
      Node* node = table_[i]->head[idx];
      while (node) {
        if (equal_(key, node->data)) return node;
        node = node->next;
      }
    }
    return nullptr;
  }

  bool Delete(std::shared_ptr<T> key) {
    if (table_[0]->used + table_[1]->used == 0) {
      return false;
    }

    uint64_t h = hash_(key);
    for (int i = 0; i < 2; i++) {
      if (table_[i]->size == 0) continue;

      uint64_t idx = h & table_[i]->sizemask;
      Node* node = table_[i]->head[idx];
      Node* prev = nullptr;
      while (node) {
        if (equal_(key, node->data)) {
          if (prev == nullptr) {
            table_[i]->head[idx] = node->next;
          } else {
            prev->next = node->next;
          }
          node->next = nullptr;
          return true;
        }
        prev = node;
        node = node->next;
      }
    }
    return false;
  }

  size_t Size() {
    //...
    return table_[0]->used + table_[1]->used;
  }

  void Insert(Node* node) {
    this->Expand();

    uint64_t idx = hash_(node->data) & table_[0]->sizemask;

    node->next = table_[0]->head[idx];
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
    table_[0]->head = (Node**)malloc(sizeof(Node*) * resize);
    memset(table_[0]->head, 0, sizeof(Node*) * resize);
    if (table_[1]->used > 0) rehashidx_ = 0;
  }

  bool RehashStep() {
    if (rehashidx_ != -1) {
      Rehash(1);
      return true;
    }
    return false;
  }

  void Rehash(int n) {
    if (table_[1]->size == 0) {
      return;
    }

    while (n > 0 && rehashidx_ < table_[1]->size) {
      Node* node = table_[1]->head[rehashidx_];
      while (node) {
        Node* next_ = node->next;
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
  Table* table_[2];
  int32_t rehashidx_;
  std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)> equal_;
  std::function<uint64_t(std::shared_ptr<T>)> hash_;
};
}  // namespace rockin
