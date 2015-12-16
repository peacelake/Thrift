/**
 * This Thrift file can be included by other Thrift files that want to share
 * these definitions.
 */

namespace cpp shared
namespace d share // "shared" would collide with the eponymous D keyword.
//namespace dart shared
namespace java shared
namespace perl shared
namespace php shared
namespace haxe shared

namespace java thrift.demo.gen
namespace py thrift.demo

struct SharedStruct {
  1: i32 key
  2: string value
}

service SharedService {
  SharedStruct getStruct(1: i32 key)
}

struct User{
    1: i32 id,
    2: string username,
    3: string password
}

exception UserNotFound{
    1:string message
}

service UserService{
    list<User> getUsers(),
    User getUserByName(1:string username) throws(1:UserNotFound unf)
}

／／thrift-0.6.1.exe --gen java  user.thrift
／／thrift-0.6.1.exe --gen py user.thrift
