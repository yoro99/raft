package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Req any
}

// StateMachine 一个想要通过复制来实现容错的服务器（例如 ../server.go）需要调用MakeRSM，并且必须实现 StateMachine 接口。
// 该接口使 rsm 包能够与服务器进行交互，以执行与服务器相关的操作：
// 服务器需要实现 DoOp，用于执行具体操作（例如 Get 或 Put 请求），
// 以及实现 Snapshot/Restore，用于对服务器状态进行快照和恢复。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type Response struct {
	result any
	term   int
}

type OpContext struct {
	result    any
	commandId int
}
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	chMap   map[int]chan Response
	opCache map[int64]OpContext
}

// MakeRSM servers[] 包含一组服务器的端口，这些服务器将通过 Raft 协作，共同构建一个具备容错能力的 key/value 服务。
// me 是当前服务器在 servers[] 数组中的索引。
// k/v 服务器应该通过底层的 Raft 实现来存储快照（snapshot）。
// Raft 在保存状态时应调用 persister.SaveStateAndSnapshot()，
// 以原子方式同时保存 Raft 的状态以及对应的快照。
// 当 Raft 已保存的状态大小超过 maxraftstate 字节时，RSM（Replicated State Machine，复制状态机）应该生成快照，
// 这样可以让 Raft 对日志进行垃圾回收（garbage collection）。如果 maxraftstate 为 -1，则不需要生成快照。
// MakeRSM() 必须快速返回，因此任何耗时较长的工作都应该在新的 goroutine 中启动执行。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		chMap:        make(map[int]chan Response),
		opCache:      make(map[int64]OpContext),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	dPrintf("make a RSM:%d", me)
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) isDuplicatedCommand(clientId int64, commandId int) (any, bool) {
	opContext, ok := rsm.opCache[clientId]
	if ok && opContext.commandId >= commandId {
		return opContext.result, true
	}
	return nil, false
}

// Submit 向 Raft 提交一个命令，并等待该命令被提交（commit）。
// 如果客户端需要寻找新的 leader 并重新尝试，则应返回 ErrWrongLeader。
// Submit 会创建一个 Op 结构体，用于通过 Raft 执行一个命令；
// 例如：op := Op{Me: rsm.me, Id: id, Req: req}，其中 req是传递给 Submit 的参数，而 id 是该操作（op）的唯一标识。
// me: submit的server/rsm对于client是唯一的，需要由提交submit的server去响应client???
func (rsm *RSM) Submit(req any) (resCode rpc.Err, res any) {
	// your code here
	cmd := Op{
		Me:  rsm.me,
		Req: req,
	}
	index, term, isLeader := rsm.rf.Start(cmd)

	rsm.mu.Lock()
	//if result, ok := rsm.isDuplicatedCommand(req.clientId, req.commandId); ok { // todo:读不需要考虑重复吗？？
	//	resCode, res = rpc.OK, result
	//	rsm.mu.Unlock()
	//	return
	//}
	rsm.mu.Unlock()
	// todo：检查是否有结果
	dPrintf("Submit me:%d req:%v index:%d term:%d isLeader:%v",
		rsm.me, stringify(req), index, term, isLeader)
	resCode, res = rpc.ErrWrongLeader, nil
	//for isLeader && term == currentTerm {
	dPrintf("yww1")
	ch := rsm.getNotifyCh(index)
	defer func() {
		go rsm.closeNotifyCh(index)
	}()
	//for isLeader {
	//	dPrintf("yyww1.1")
	//	select {
	//	case response := <-ch:
	//		dPrintf("yww3")
	//	if response.term == currentTerm {
	//		resCode, res = rpc.OK, response.result
	//	}
	//		return
	//	//break 的默认行为是仅跳出最内层的控制结构。由于 select 本身也是一种控制结构，当你写下 break 时，它只会结束当前的 select 代码块，而外层的 for 循环会继续执行下一次迭代。
	//	case <-time.After(1500 * time.Millisecond): //todo: channel 返回isleader结果
	//		//	resCode, res = rpc.ErrWrongLeader, nil
	//		currTerm, currIsLeader := rsm.rf.GetState()
	//		dPrintf("Submit timeout cTerm:%d, term:%d, cLeader:%v, resCode:%v",
	//			currTerm, term, currIsLeader, resCode)
	//		if currTerm != term || currIsLeader == false {
	//			isLeader = false
	//		}
	//	}
	//}
	if isLeader {
		select {
		case response := <-ch:
			dPrintf("yww3")
			if response.term == term {
				resCode, res = rpc.OK, response.result
			}
			return
		case <-time.After(1500 * time.Millisecond): //todo: channel 返回isleader结果
			resCode, res = rpc.ErrWrongLeader, nil
		}
	}
	return resCode, res // I'm dead, try another server.
}

// yw: 需要保证执行一致性
// 一致性应该需要通过rsm来完成
func (rsm *RSM) applier() {
	for rsm.rf.Killed() == false {
		select {
		case response := <-rsm.applyCh:
			if response.CommandValid {
				dPrintf("applier me:%d index:%d term:%d cmd:%v\n", rsm.me, response.CommandIndex, response.CommandTerm, response.Command)
				// leader才需要响应client（server响应作为leader时期的client）
				op := response.Command.(Op)
				result := rsm.sm.DoOp(op.Req)
				// todo： 存取结果
				//if currentTerm, isLeader := rsm.rf.GetState(); isLeader && currentTerm == response.CommandTerm {
				//	ch := rsm.getNotifyCh(response.CommandIndex)
				//	ch <- result
				//}
				if op.Me == rsm.me {
					dPrintf("yww2")
					ch := rsm.getNotifyCh(response.CommandIndex)
					ch <- Response{result: result, term: response.CommandTerm}
				}
			}
		}
	}
}

func (rsm *RSM) getNotifyCh(index int) chan Response {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if ch, ok := rsm.chMap[index]; ok {
		return ch
	}
	ch := make(chan Response)
	rsm.chMap[index] = ch
	return ch
}

func (rsm *RSM) closeNotifyCh(index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if ch, ok := rsm.chMap[index]; ok { // 确保关闭的ch存在
		delete(rsm.chMap, index)
		close(ch)
	}
}
