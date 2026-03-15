package kvraft

import (
	"math/big"
	"math/rand"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientId      int64
	commandSeqNum int
	leader        int
}

func randN() int64 {
	maxN := big.NewInt(int64(1) << 62)
	num := rand.Int63n((*maxN).Int64())
	return num
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:          clnt,
		servers:       servers,
		clientId:      randN(),
		commandSeqNum: 0,
		leader:        0,
	}
	return ck
}

// Get 获取指定 key 的当前值和值的版本号。如果 key 不存在，返回 ErrNoKey。
// 在遇到其他错误时，它会一直重试，直到成功为止。
// 你可以用如下方式向服务器 i 发送 RPC 调用：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数声明的参数类型一致。此外，reply 必须以指针形式传入。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := CommandArgs{
		ClientId:  ck.clientId,
		CommandId: ck.commandSeqNum,
		Key:       key,
	}
	reply := CommandReply{}
	ck.Call("KVServer.Get", &args, &reply)
	ck.commandSeqNum += 1
	return reply.Value, reply.Version, reply.Status
}

// Put 会更新指定 key 的值，但仅在请求中的版本号与服务器上该 key 的版本号匹配时才会更新。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果 Put 在第一次 RPC 就收到 ErrVersion，应直接返回 ErrVersion，因为此时 Put 肯定没有在服务器上执行。
// 如果在重试的 RPC 中服务器返回 ErrVersion，则 Put 必须返回 ErrMaybe 给应用程序，因为之前的 RPC 可能已经在服务器上成功执行，但响应丢失，
// Clerk 无法确定该 Put 是否真正执行。
// 你可以用如下方式向服务器 i 发送 RPC 调用：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数声明的参数类型一致。此外，reply 必须以指针形式传入。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := CommandArgs{
		ClientId:  ck.clientId,
		CommandId: ck.commandSeqNum,
		Key:       key,
		Value:     value,
		Version:   version,
	}
	reply := CommandReply{}
	cnt := ck.Call("KVServer.Put", &args, &reply)
	if cnt > 0 && reply.Status == rpc.ErrVersion {
		reply.Status = rpc.ErrMaybe
	}
	ck.commandSeqNum += 1
	return reply.Status
}

func (ck *Clerk) Call(method string, args *CommandArgs, reply *CommandReply) int {
	cnt := 1
	for {
		if ok := ck.clnt.Call(ck.servers[ck.leader], method, args, reply); ok {
			if reply.Status == rpc.ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue
			} else {
				break
			}
		}
		cnt += 1
	}
	return cnt
}
