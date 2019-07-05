#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "utils.h"

namespace rockin {
class RockinConn;
class RedisArgs;

// GET key
extern void GetCommand(std::shared_ptr<RedisArgs> cmd);

// SET key value
extern void SetCommand(std::shared_ptr<RedisArgs> cmd);

// APPEND key value
extern void AppendCommand(std::shared_ptr<RedisArgs> cmd);

// GETSET key value
extern void GetSetCommand(std::shared_ptr<RedisArgs> cmd);

// MGET key1 ....
extern void MGetCommand(std::shared_ptr<RedisArgs> cmd);

// MSET key1 value1 ....
extern void MSetCommand(std::shared_ptr<RedisArgs> cmd);

// INCR key
extern void IncrCommand(std::shared_ptr<RedisArgs> cmd);

// INCRBY key value
extern void IncrbyCommand(std::shared_ptr<RedisArgs> cmd);

// DECR key
extern void DecrCommand(std::shared_ptr<RedisArgs> cmd);

// DECR key value
extern void DecrbyCommand(std::shared_ptr<RedisArgs> cmd);

// SETBIT key offset value
extern void SetBitCommand(std::shared_ptr<RedisArgs> cmd);

// GETBIT key offset
extern void GetBitCommand(std::shared_ptr<RedisArgs> cmd);

// BITCOUNT key [start end]
extern void BitCountCommand(std::shared_ptr<RedisArgs> cmd);

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
extern void BitOpCommand(std::shared_ptr<RedisArgs> cmd);

// BITPOS key bit[start][end]
extern void BitPosCommand(std::shared_ptr<RedisArgs> cmd);

}  // namespace rockin
