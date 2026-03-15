package kvraft

import "6.5840/kvsrv1/rpc"

type CommandArgs struct {
	ClientId  int64
	CommandId int
	Key       string
	Value     string
	Version   rpc.Tversion
}

type CommandReply struct {
	Status  rpc.Err
	Value   string
	Version rpc.Tversion
}
