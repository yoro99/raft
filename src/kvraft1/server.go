package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	kvCache map[string]CValue
	mu      sync.Mutex
}

type CValue struct {
	value   string
	version rpc.Tversion
}

type GetReq struct {
	clientId  int64
	commandId int
	key       string
}

type PutReq struct {
	clientId  int64
	commandId int
	key       string
	value     string
	version   rpc.Tversion
}

type Resp struct {
	status  rpc.Err
	value   string
	version rpc.Tversion
}

// DoOp 要将 req 类型转换为正确的类型，可以查看下面 Go 的类型开关（type switch）
// 或类型断言（type assertion）的相关内容：
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// 这个做去重？
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req.(type) {
	case GetReq:
		if v, ok := kv.kvCache[req.(GetReq).key]; ok {
			return Resp{status: rpc.OK, value: v.value, version: v.version}
		}
		return Resp{status: rpc.ErrNoKey}
	case PutReq:
		putReq := req.(PutReq)
		if v, ok := kv.kvCache[putReq.key]; ok && v.version != putReq.version {
			return Resp{status: rpc.ErrVersion}
		}
		kv.kvCache[putReq.key] = CValue{value: putReq.value, version: putReq.version}
		return Resp{status: rpc.OK}
	default:
		panic("Error op")
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *CommandArgs, reply *CommandReply) {
	// 在这里编写你的代码。使用 kv.rsm.Submit() 来提交 args。
	// 你可以使用 Go 的类型转换（type cast），将Submit()返回的any类型转换为GetReply：rep.(rpc.GetReply)
	req := GetReq{
		clientId:  args.ClientId,
		commandId: args.CommandId,
		key:       args.Key,
	}
	status, resp := kv.rsm.Submit(req)
	if status == rpc.ErrWrongLeader {
		reply.Status = status
	} else {
		getResp := resp.(Resp)
		reply.Status, reply.Value, reply.Version = getResp.status, getResp.value, getResp.version
	}
}

func (kv *KVServer) Put(args *CommandArgs, reply *CommandReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := PutReq{
		clientId:  args.ClientId,
		commandId: args.CommandId,
		key:       args.Key,
		value:     args.Value,
		version:   args.Version,
	}
	status, resp := kv.rsm.Submit(req)
	if status == rpc.ErrWrongLeader {
		reply.Status = status
	} else {
		putResp := resp.(Resp)
		reply.Status = putResp.status
	}
}

// 当某个 KVServer 实例不再需要时，测试程序会调用 Kill()。
// 为了方便，我们提供了一段代码用于设置 rf.dead（不需要加锁），以及一个 killed() 方法，用于在长时间运行的循环中检查 rf.dead。
// 你也可以在 Kill() 中添加自己的代码。
// 你并不必须对这个函数做任何处理，但这样做可能会更方便（例如）用来在实例被 Kill() 后停止输出调试日志。
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer StartKVServer()必须快速返回，因此任何需要长时间运行的工作都应该在新的 goroutine 中启动执行。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	/// 对你希望 Go 的 RPC 库进行序列化/反序列化（marshall/unmarshall）的结构体
	// 调用 labgob.Register。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:      me,
		kvCache: make(map[string]CValue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
