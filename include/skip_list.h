#include <cmath>
#include <functional>
#include <memory>

const float P = 0.5;

template <typename T, int MAX_LEVEL = 5>
class SkipList {
  struct Node {
    std::shared_ptr<T> value_;
    Node **link_;

    Node(int level, std::shared_ptr<T> value) {
      link_ = new Node *[level + 1];
      memset(link_, 0, sizeof(Node *) * (level + 1));
      this->value_ = value;
    }

    ~Node() {
      delete[] link_;
      value_ = nullptr;
    }
  };

 public:
  SkipList(std::function<int(std::shared_ptr<T>, std::shared_ptr<T>)> compare,
           std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)> equal)
      : compare_(compare), equal_(equal) {
    header_ = new Node(MAX_LEVEL, nullptr);
    level_ = 0;
  }

  ~SkipList() { delete header_; }

  void Insert(std::shared_ptr<T> value) {
    Node *x = header_;
    Node *update[MAX_LEVEL + 1];
    memset(update, 0, sizeof(Node *) * (MAX_LEVEL + 1));
    for (int i = level_; i >= 0; i--) {
      while (x->link_[i] != NULL && compare_(value, x->link_[i]->value_) > 0) {
        x = x->link_[i];
      }
      update[i] = x;
    }
    x = x->link_[0];
    if (x == NULL || !equal_(value, x->value_)) {
      int lvl = RandomLevel();
      if (lvl > level_) {
        for (int i = level_ + 1; i <= lvl; i++) {
          update[i] = header_;
        }
        level_ = lvl;
      }
      x = new Node(lvl, value);
      for (int i = 0; i <= lvl; i++) {
        x->link_[i] = update[i]->link_[i];
        update[i]->link_[i] = x;
      }
    }
  }

  void Delete(std::shared_ptr<T> value) {
    Node *x = header_;
    Node *update[MAX_LEVEL + 1];
    memset(update, 0, sizeof(Node *) * (MAX_LEVEL + 1));
    for (int i = level_; i >= 0; i--) {
      while (x->link_[i] != NULL && compare_(value, x->link_[i]->value_) > 0) {
        x = x->link_[i];
      }
      update[i] = x;
    }
    x = x->link_[0];
    if (x && equal_(value, x->value_)) {
      for (int i = 0; i <= level_; i++) {
        if (update[i]->link_[i] != x) break;
        update[i]->link_[i] = x->link_[i];
      }
      delete x;
      while (level_ > 0 && header_->link_[level_] == NULL) {
        level_--;
      }
    }
  }

  bool Contains(std::shared_ptr<T> value) {
    Node *x = header_;
    for (int i = level_; i >= 0; i--) {
      while (x->link_[i] != NULL && compare_(value, x->link_[i]->value_) > 0) {
        x = x->link_[i];
      }
    }
    x = x->link_[0];
    return x != NULL && equal_(value, x->value_);
  }

  void Range(std::function<bool(std::shared_ptr<T>)> callback) {
    const Node *x = header_->link_[0];
    while (x != NULL) {
      if (!callback(x->value_)) break;
      x = x->link_[0];
    }
  }

 private:
  int RandomLevel() {
    static bool first = true;
    if (first) {
      srand((unsigned)time(NULL));
      first = false;
    }
    int lvl = (int)(log((float)rand() / RAND_MAX) / log(1. - P));
    return lvl < MAX_LEVEL ? lvl : MAX_LEVEL;
  }

 private:
  std::function<int(std::shared_ptr<T>, std::shared_ptr<T>)> compare_;
  std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)> equal_;
  Node *header_;
  int level_;
};
