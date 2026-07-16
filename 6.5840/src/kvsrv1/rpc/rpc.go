package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	ClntId  int64 // 客户端唯一id ，MakeClerk 时随机生成
	SeqNum  int64 // 客户的请求编号， 每次 Get/Put 前 +1
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	ClntId int64 // 客户端唯一id ，MakeClerk 时随机生成
	SeqNum int64 // 客户的请求编号， 每次 Get/Put 前 +1
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
