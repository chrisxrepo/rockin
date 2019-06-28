# rockin

#### 依赖包安装
##### Centos7
```
yum install -y epel-release && rpm -ql epel-release
yum install -y gflags-devel glog-devel jemalloc-devel libuv-devel
```
##### Ubuntu18.10
```
apt-get install  -y libgflags-dev libgoogle-glog-dev libjemalloc-dev libuv1-dev 
```
##### MacOs
``` 
brew install gflags glog jemalloc libuv 
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
```