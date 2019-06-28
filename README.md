# rockin

#### 依赖包安装
##### Centos7
```
yum install -y epel-release && rpm -ql epel-release
yum install -y gflags-devel glog-devel jemalloc-devel libuv-devel
```
##### Ubuntu18.10
```
apt-get install libgflags-dev libgoogle-glog-dev libjemalloc-dev libuv1-dev 
```
##### MacOs
``` 
brew install gflags glog jemalloc libuv 
```