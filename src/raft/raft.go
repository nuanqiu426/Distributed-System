package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"encoding/json"
	"math/rand"
	"sort"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	//For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nPeers int

	timeoutInterval time.Duration
	lastActiveTime  time.Time

	//2A
	term     int
	role     MemberRole
	leaderId int
	votedFor int

	logs []*LogEntry
	//2B
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int

	//2D
	lastIncludeIndex int //快照的最后一条Log的logIndex
	lastIncludeTerm  int //快照的最后一条Log的logTerm
	snapshotOffset   int //快照可能分批次传输
	snapshot         []byte

	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type MemberRole int

const (
	Leader    MemberRole = 1
	Follower  MemberRole = 2
	Candidate MemberRole = 3
	RoleNone  int        = 4
)

type LogType int

const (
	HeartBeatLogType   LogType = 1
	AppendEntryLogType LogType = 2
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	HeartBeatTimeOut = 50 * time.Millisecond
	ElectionTimeOut  = 150 * time.Millisecond
)

func randElectionTime() time.Duration {
	return ElectionTimeOut + time.Duration(rand.Uint32())%ElectionTimeOut
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term, rf.role == Leader
}

func (rf *Raft) GetLastTermAndIndex() (int, int) {
	logIndex := rf.GetLastIndex()
	logTerm := rf.logs[logIndex-rf.lastIncludeIndex].Term
	return logTerm, logIndex
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}
func (rf *Raft) GetTermByIndex(logIndex int) int {
	return rf.logs[logIndex].Term
}

// logIndex入参是无损索引, 要映射到压缩后的索引
func (rf *Raft) mapIndex(logIndex int) *LogEntry {
	logIndex = logIndex - rf.lastIncludeIndex
	if logIndex > rf.GetLastIndex() || logIndex <= 0 {
		return rf.logs[0]
	}
	return rf.logs[logIndex]
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Any2String(data interface{}) string {
	marshal, _ := json.Marshal(data)
	return string(marshal)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// 心跳+日志追加+发送快照
type AppendEntriesArgs struct {
	LogType  LogType
	LeaderId int
	Term     int //leader currentTerm
	//用于日志复制，确保前面日志能够匹配
	PrevLogTerm         int
	PrevLogIndex        int
	LeaderCommitedIndex int
	LogEntries          []*LogEntry
}

type AppendEntriesReply struct {
	Success bool
	Term    int
	//用于探测日志匹配点
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term             int //leader's term
	LeaderId         int
	LastIncludeIndex int //snapshot中最后一条日志的index
	LastIncludeTerm  int
	Data             []byte
	//Offset           int //此次传输chunk在快照文件的偏移量，快照文件可能很大，因此需要分chunk，此次不分片
	//Done             bool //是否最后一块
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		DPrintf("node[%d] role[%v] received vote request from node[%d], now[%d], args: %v, reply: %v", rf.me, rf.role, args.CandidateId, time.Now().UnixMilli(), Any2String(args), Any2String(reply))
	}()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	// 投票约束1
	if rf.term > args.Term {
		return
	}

	if args.Term > rf.term {
		rf.role = Follower //leader转换为follower
		rf.term = args.Term
		//需要比较最新一条日志的情况再决定要不要投票
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}

	//避免重复投票
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastLogTerm, GetLastIndex := rf.GetLastTermAndIndex()
		//最后一条日志任期更大或者任期一样但是更长
		// 投票约束2
		if args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && GetLastIndex <= args.LastLogIndex) {
			rf.role = Follower
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTime()
			reply.VoteGranted = true
			rf.persist()
		}
	}
}

// 如果收到term比自己大的AppendEntries请求，则表示发生过新一轮的选举，此时拒绝掉，等待超时选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("process AppendEntries node[%d] role[%v] from node[%d] term[%d] args: %v, reply: %v", rf.me, rf.role, args.LeaderId, args.Term, Any2String(args), Any2String(reply))
	}()

	reply.Term = rf.term
	reply.Success = false
	//拒绝旧leader请求
	if args.Term < rf.term {
		DPrintf("appendEntries node[%v] term[%d] from node[%v] term[%d], args.Term < rf.term declined", rf.me, rf.term, args.LeaderId, args.Term)
		return
	}
	//发现一个更大的任期，转变成这个term的follower，leader、follower--> follower
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		//发现term大于等于自己的日志复制请求，则认其为主
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.lastActiveTime = time.Now()
	//还缺少前面的日志或者前一条日志匹配不上
	if args.PrevLogIndex > rf.GetLastIndex() {
		reply.NextIndex = rf.GetLastIndex() + 1
		DPrintf("appendEntries node[%d] term[%d] GetLastIndex[%d] from node[%d], args.PrevLogIndex > rf.GetLastIndex() declined, args: %v reply: %v ", rf.me, rf.term, rf.GetLastIndex(), args.LeaderId, Any2String(args), Any2String(reply))
		return
	}
	//本peer压缩过，不能再往前探测，压缩的日志一定是已经提交的日志
	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.NextIndex = rf.lastIncludeIndex + 1
		//nextIndex向后移动了就算success
		return
	}
	if args.PrevLogTerm != rf.GetTermByIndex(args.PrevLogIndex-rf.lastIncludeIndex) {
		//前一条日志的任期不匹配，找到冲突term首次出现的地方
		index := args.PrevLogIndex - rf.lastIncludeIndex
		term := rf.GetTermByIndex(index)
		for index > 0 && rf.GetTermByIndex(index) == term {
			index--
		}
		reply.NextIndex = index + 1 + rf.lastIncludeIndex
		DPrintf("AppendEntries node[%v] term[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]  from node[%v] term[%d], args.PrevLogTerm != rf.GetTermByIndex(args.PrevLogIndex-rf.lastIncludeIndex) declined, reply: %v, args: %v", rf.me, rf.term, rf.lastIncludeIndex, rf.lastIncludeTerm, args.LeaderId, args.Term, Any2String(reply), Any2String(args))
		return
	}
	//args.PrevLogIndex<=GetLastIndex，有可能发生截断的情况
	if rf.GetLastIndex() > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex]
	}
	rf.logs = append(rf.logs, args.LogEntries...)
	// len(自己能确认提交的log数目) = min(len(自己拥有的log数目),len(全局确认能被确认提交的log数目))
	rf.commitIndex = min(rf.GetLastIndex(), args.LeaderCommitedIndex)
	rf.persist()
	reply.Success = true
	rf.matchIndex[rf.me] = rf.GetLastIndex()
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	reply.NextIndex = rf.nextIndex[rf.me]
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 10)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Leader {
				return
			}
			//如果没有超时或者没有需要发送的数据，则直接返回
			if time.Now().Sub(rf.lastActiveTime) < HeartBeatTimeOut {
				return
			}
			rf.lastActiveTime = time.Now()
			rf.matchIndex[rf.me] = rf.GetLastIndex()
			rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
			//DPrintf("heartBeatLoop node[%d] role[%v] term[%d] GetLastIndex[%v] matchIndex[%v], commitIndex[%d] log: %v\n", rf.me, rf.role, rf.term, rf.GetLastIndex(), Any2String(rf.matchIndex), rf.commitIndex, Any2String(rf.logs))
			for i := 0; i < rf.nPeers; i++ {
				if rf.me == i {
					continue
				}
				//日志在快照点之前，发送快照
				if rf.nextIndex[i] <= rf.lastIncludeIndex {
					argsI := &InstallSnapshotArgs{
						Term:             rf.term,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludeTerm,
						Data:             rf.snapshot,
					}
					go func(server int, args *InstallSnapshotArgs) {
						reply := &InstallSnapshotReply{}
						ok := rf.sendInstallSnapshot(server, args, reply)
						if !ok || rf.role != Leader {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						//发现更大的term，本结点是旧leader
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = Follower
							rf.persist()
							return
						}
						//follower拒绝snapshot证明其commitIndex>lastIncludedIndex，接收也可以使得其commitIndex>lastIncludedIndex
						rf.matchIndex[server] = rf.lastIncludeIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						matchIndexSlice := make([]int, rf.nPeers)
						for index, matchIndex := range rf.matchIndex {
							matchIndexSlice[index] = matchIndex
						}
						sort.Slice(matchIndexSlice, func(i, j int) bool {
							return matchIndexSlice[i] < matchIndexSlice[j]
						})
						newCommitIndex := matchIndexSlice[rf.nPeers/2]
						//不能提交不属于当前term的日志
						DPrintf("id[%d] role[%v] snapshot commitIndex %v update to newcommitIndex %v, lastSnapshotIndex %v,  command: %v, matchIndex: %v\n", rf.me, rf.role, rf.commitIndex, newCommitIndex, rf.lastIncludeIndex, 0, Any2String(rf.matchIndex))
						if newCommitIndex > rf.commitIndex && rf.GetTermByIndex(newCommitIndex-rf.lastIncludeIndex) == rf.term {
							//如果commitIndex比自己实际的日志长度还大，这时需要减小
							rf.commitIndex = min(rf.GetLastIndex(), newCommitIndex)
						}

					}(i, argsI)
				} else {
					//记录每个node本次发送日志的前一条日志
					prevLogIndex := min(rf.matchIndex[i], rf.GetLastIndex())
					//发送日志
					//有可能follower的matchIndex比leader还大，此时要担心是否越界
					DPrintf("sendAppendEntries node[%d] role[%v] term[%d] GetLastIndex[%d] matchIndex[%v], lastSnapshotIndex[%v], log: %v\n", rf.me, rf.role, rf.term, rf.GetLastIndex(), Any2String(rf.matchIndex), rf.lastIncludeIndex, Any2String(rf.logs))
					argsI := &AppendEntriesArgs{
						LogType:             HeartBeatLogType,
						Term:                rf.term,
						LeaderId:            rf.me,
						PrevLogIndex:        prevLogIndex,
						PrevLogTerm:         rf.GetTermByIndex(prevLogIndex - rf.lastIncludeIndex),
						LeaderCommitedIndex: rf.commitIndex, //对上一次日志复制请求的二阶段
					}

					//本次复制的最后一条日志
					if rf.matchIndex[i] < rf.GetLastIndex() {
						argsI.LogType = AppendEntryLogType
						argsI.LogEntries = make([]*LogEntry, 0)
						//因为此时没有加锁，担心有新日志写入，必须保证每个节点复制的最后一条日志一样才能达到过半提交的效果
						argsI.LogEntries = append(argsI.LogEntries, rf.logs[rf.nextIndex[i]-rf.lastIncludeIndex:]...)
					}

					go func(server int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, args, reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						//如果term变了，表示该结点不再是leader，什么也不做
						if !ok || rf.role != Leader {
							return
						}
						//发现更大的term，本结点是旧leader
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = Follower
							rf.persist()
							return
						}
						//follower缺少的之前的日志，探测缺少的位置
						//后退策略
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = reply.NextIndex - 1
						if reply.Success {
							//提交到哪个位置需要根据中位数来判断，中位数表示过半提交的日志位置，
							matchIndexSlice := make([]int, rf.nPeers)
							for index, matchIndex := range rf.matchIndex {
								matchIndexSlice[index] = matchIndex
							}
							sort.Slice(matchIndexSlice, func(i, j int) bool {
								return matchIndexSlice[i] < matchIndexSlice[j]
							})
							newCommitIndex := matchIndexSlice[rf.nPeers/2]
							if newCommitIndex > rf.commitIndex && rf.GetTermByIndex(newCommitIndex-rf.lastIncludeIndex) == rf.term {
								rf.commitIndex = min(rf.GetLastIndex(), newCommitIndex)
							}
						}
					}(i, argsI)
				}

			}
		}()
	}
}

func (rf *Raft) applyLogLoop() {

	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		//applyMsgs := make([]ApplyMsg, 0)
		var applyMsg *ApplyMsg
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//没有数据需要上传给应用层
			if rf.lastApplied >= rf.commitIndex {
				return
			}
			if rf.lastApplied < rf.lastIncludeIndex {
				rf.lastApplied = rf.lastIncludeIndex
			}
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsg = &ApplyMsg{
					CommandValid: true,
					Command:      rf.mapIndex(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.mapIndex(rf.lastApplied).Term,
				}
			}
		}()
		if applyMsg != nil {
			go func() {
				//锁外提交给应用
				rf.applyChan <- *applyMsg
				DPrintf("id[%v] role[%v] upload log to application, lastApplied[%d], commitIndex[%d]", rf.me, rf.role, applyMsg.CommandIndex, rf.commitIndex)
			}()
		}
	}
}
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 1)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				return
			}
			if time.Now().Sub(rf.lastActiveTime) < rf.timeoutInterval {
				//不超时不需要进入下一步，只需要接收RequestVote和AppendEntries请求即可
				return
			}
			//超时处理逻辑
			if rf.role == Follower {
				rf.role = Candidate
			}
			DPrintf("become candidate... node[%v] term[%v] role[%v] lastActiveTime[%v], timeoutInterval[%d], now[%v]", rf.me, rf.term, rf.role, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds(), time.Now().Sub(rf.lastActiveTime).Milliseconds())
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTime()
			rf.votedFor = rf.me
			rf.term++
			rf.persist()
			lastLogTerm, GetLastIndex := rf.GetLastTermAndIndex()
			rf.mu.Unlock()

			maxTerm, voteGranted := rf.becomeCandidate(GetLastIndex, lastLogTerm)
			rf.mu.Lock()
			DPrintf("node[%d] get vote num[%d]", rf.me, voteGranted)

			//在这过程中接收到更大term的请求，导致退化为follower
			if rf.role != Candidate {
				DPrintf("node[%d] role[%v] failed to leader, voteGranted[%d]", rf.me, rf.role, voteGranted)
				return
			}
			if maxTerm > rf.term {
				rf.role = Follower
				rf.term = maxTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
				rf.persist()
			} else if voteGranted > rf.nPeers/2 {
				rf.leaderId = rf.me
				rf.role = Leader
				rf.lastActiveTime = time.Unix(0, 0)
				rf.persist()
			}
			DPrintf("node[%d] role[%v] maxTerm[%d] voteGranted[%d] nPeers[%d]", rf.me, rf.role, maxTerm, voteGranted, rf.nPeers)
		}()
	}
}

func (rf *Raft) becomeCandidate(GetLastIndex, lastLogTerm int) (int, int) {

	type RequestVoteResult struct {
		peerId int
		resp   *RequestVoteReply
	}
	voteChan := make(chan *RequestVoteResult, rf.nPeers-1)
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: GetLastIndex,
		LastLogTerm:  lastLogTerm,
	}
	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   reply,
				}
			} else {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   nil,
				}
			}
		}(i, args)
	}

	maxTerm := rf.term
	voteGranted := 1
	totalVote := 1
	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case vote := <-voteChan:
			totalVote++
			if vote.resp != nil {
				if vote.resp.VoteGranted {
					voteGranted++
				}
				//出现更大term就退回follower
				if vote.resp.Term > maxTerm {
					maxTerm = vote.resp.Term
				}
			}
		}
		if voteGranted > rf.nPeers/2 || totalVote == rf.nPeers {
			return maxTerm, voteGranted
		}
	}
	return maxTerm, voteGranted
}

// 写入数据
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	entry := &LogEntry{
		Term:    rf.term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	index := rf.GetLastIndex()
	term := rf.term
	//写入后立刻持久化
	rf.persist()
	DPrintf("node[%d] term[%d] role[%v] add entry: %v, logIndex[%d]", rf.me, rf.term, rf.role, Any2String(entry), index)
	return index, term, true
}

// 初始化raft, 所有raft的任务都要另起协程，测试文件采用的是协程模拟rpc
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      -1,
		nPeers:    len(peers),

		leaderId:       RoleNone,
		term:           0,
		votedFor:       RoleNone,
		role:           Follower,
		lastActiveTime: time.Now(),
		//lastHeartBeatTime: time.Now(),
		timeoutInterval: randElectionTime(),
		commitIndex:     0,
		lastApplied:     0,
		applyChan:       applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("starting new raft node, id[%d], lastActiveTime[%v], timeoutInterval[%d]", me, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds())
	//2B
	rf.logs = make([]*LogEntry, 0)
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	// 2D dummy head
	rf.logs = append(rf.logs, &LogEntry{
		Term: 0,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()
	go rf.applyLogLoop()

	DPrintf("starting raft node[%d]", rf.me)

	return rf
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// 外层加锁，内层不能够再加锁了
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
}
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//持久化当前term以及是否给其他结点投过票，避免同一个term多次投票的情况
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.leaderId)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	return w.Bytes()
}

// restore previously persisted state.
//
// 一般刚刚启动时执行
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	d.Decode(&rf.term)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.leaderId)
	d.Decode(&rf.logs)
	d.Decode(&rf.lastIncludeIndex)
	rf.lastIncludeTerm = rf.logs[0].Term
	rf.mu.Unlock()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// 应用层调用，询问Raft是否需要安装这个snapshot，在InstallSnapshot时并不会安装
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//异步应用快照，如果此时commitIndex已经追上来了，就不需要再应用快照了
	if rf.commitIndex > lastIncludedIndex {
		return false
	}
	logs := rf.logs[0:1]
	logs[0].Term = lastIncludedTerm
	//本结点最后一条日志在快照点之前，太落后，清空，应用快照，否则截断
	if rf.GetLastIndex() > lastIncludedIndex {
		logs = append(logs, rf.logs[lastIncludedIndex-rf.lastIncludeIndex+1:]...)
	}
	rf.logs = logs
	rf.snapshot = snapshot
	rf.lastIncludeIndex = lastIncludedIndex
	rf.lastIncludeTerm = lastIncludedTerm

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	return true
}

// RPC: 接收来自Leader的快照，并上传给应用层，通过applyCh写入
// 这个函数是Follower为了赶上Leader状态的，
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("process InstallSnapshot node[%v] term[%v] GetLastIndex[%d] lastLogTerm[%d] lastIncludeIndex[%d] commitIndex[%d]  received InstallSnapshot, args: %v, reply: %v", rf.me, rf.term, rf.GetLastIndex(), rf.GetTermByIndex(rf.GetLastIndex()-rf.lastIncludeIndex), rf.lastIncludeIndex, rf.commitIndex, Any2String(args), Any2String(reply))
	}()
	reply.Term = rf.term
	if rf.term > args.Term || args.Data == nil {
		DPrintf("InstallSnapshot node[%d] term[%d] from node[%d] term[%d], rf.term > args.Term delined, args: %v, reply: %v ", rf.me, rf.term, args.LeaderId, args.Term, Any2String(args), Any2String(reply))
		return
	}
	if rf.term < args.Term {
		rf.role = Follower
		rf.term = args.Term
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	rf.lastActiveTime = time.Now()
	//只有缺少的数据在快照点之前时才需要快照
	if rf.commitIndex >= args.LastIncludeIndex {
		DPrintf("InstallSnapshot node[%d] term[%d] from node[%d] term[%d], commitIndex[%d], rf.commitIndex >= args.LastIncludeIndex delined, args: %v, reply: %v ", rf.me, rf.term, args.LeaderId, args.Term, rf.commitIndex, Any2String(args), Any2String(reply))

		return
	}
	//接收快照并持久化，至于应用到状态机可以异步做，就算意外下线，也是从日志和快照恢复
	//
	//不能立刻应用快照，需要保证raft和状态机都应用快照成功，放到CondInstallSnapshot中应用
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	//异步做，及早返回，就算失败raft也会回退
	go func() {
		rf.applyChan <- applyMsg
	}()
}

// 应用层调用: index表示无损索引, snapshot是包含index之前的完整快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludeIndex || index != rf.lastApplied || index > rf.GetLastIndex() {
		return
	}
	DPrintf("node[%d] role[%d] term[%d] snapshoting, index[%d] commitIndex[%d] lastApplied[%d]", rf.me, rf.role, rf.term, index, rf.commitIndex, rf.lastApplied)
	// 若 a=[0,1,2,3,4,5] a[0:1] 将得到切片 []int{1} a[0] 将得到整数 1
	logs := rf.logs[0:1]
	logs[0].Term = rf.logs[index-rf.lastIncludeIndex].Term
	//本结点最后一条日志在快照点之前，太落后，清空，应用快照，否则截断
	logs = append(logs, rf.logs[index-rf.lastIncludeIndex+1:]...)
	rf.logs = logs
	rf.snapshot = snapshot
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = logs[0].Term
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}
