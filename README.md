# rockin

#### 依赖包安装
##### Centos7
```
yum install -y epel-release && rpm -ql epel-release
yum install -y gflags-devel glog-devel jemalloc-devel libuv-devel zlib-devel bzip2-devel snappy-devel
```
##### Ubuntu18.10
```
apt-get install  -y libgflags-dev libgoogle-glog-dev libjemalloc-dev libuv1-dev bzip2 pt-zlib1g-dev libsnappy-dev 
```
##### MacOs
``` 
brew install gflags glog jemalloc libuv bzip2 zlib snappy
```
##### Install Rocksdb 
```
git clone https://github.com/facebook/rocksdb.git 
cd rocksdb && git checkout v5.18.3 
cd rocksdb && git cherry-pick 8a1ecd1982341cfe073924d36717e11446cbe492 
cd rocksdb && DEBUG_LEVEL=0 make shared_lib && make install-shared 
echo "/usr/local/lib" | tee /etc/ld.so.conf.d/rocksdb-x86_64.conf && ldconfig 
```
#### 实现Redis命令
```
GET 
SET
MGET
MSET 
APPEND 
INCR 
DECR 
INCRBY 
DECRBY 
SETBIT 
GETBIT
BITOP
BITCOUNT
BITPOS
```