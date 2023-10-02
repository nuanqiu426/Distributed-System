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
	"encoding/json"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
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
	nPeers    int

	timeoutInterval time.Duration //follower leader candidate
	lastActiveTime  time.Time     //超时开始计算时间，收到心跳时会更新

	//选举
	term     int
	role     MemberRole
	leaderId int
	votedFor int

	//提交情况
	logs []*LogEntry

	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type MemberRole int

const (
	Leader    MemberRole = 1
	Follower  MemberRole = 2
	Candidate MemberRole = 3

	RoleNone = -1
	None     = 0
)

func (m MemberRole) String() string {
	switch m {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unknown"
}

type LogType int

const (
	HeartBeatLogType   LogType = 1
	AppendEntryLogType LogType = 2
)

func Any2String(data interface{}) string {
	marshal, _ := json.Marshal(data)
	return string(marshal)
}

type LogEntry struct {
	LogTerm int
	Command interface{}
}

const (
	ElectionTimeout = 150 * time.Millisecond
	HeatBeatTimeout = 50 * time.Millisecond
)

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
}

func heartBeatTimeout() time.Duration {
	return HeatBeatTimeout
}

// 选举时需要传递自己拥有的最后一条log的term和index
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

// 心跳或者日志追加
type AppendEntriesArgs struct {
	LogType  LogType
	LeaderId int
	Term     int //leader currentTerm

	LogEntries []*LogEntry
}

// 心跳或者日志追加
type AppendEntriesReply struct {
	Success bool
	Term    int
	Msg     string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term, rf.role == Leader
}

func (rf *Raft) lastLogTermAndLastLogIndex() (int, int) {
	logIndex := len(rf.logs) - 1
	logTerm := rf.logs[logIndex].LogTerm
	return logTerm, logIndex
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}
func (rf *Raft) logTerm(logIndex int) int {
	return rf.logs[logIndex].LogTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		DPrintf("node[%d] role[%v] received vote from node[%d], now[%d], args: %v, reply: %v", rf.me, rf.role, args.CandidateId, time.Now().UnixMilli(), Any2String(args), Any2String(reply))
	}()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	//不接收小于自己term的请求
	if rf.term > args.Term {
		return
	}

	if args.Term > rf.term {
		rf.role = Follower //leader转换为follower
		rf.term = args.Term
		//需要比较最新一条日志的情况再决定要不要投票
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
	}
	//避免重复投票
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
		//最后一条日志任期更大或者任期一样但是更长
		if args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			rf.role = Follower
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			reply.VoteGranted = true
		}
	}
}

// 如果收到term比自己大的AppendEntries请求，则表示发生过新一轮的选举，此时拒绝掉，等待超时选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("id[%d] role[%v] args: %v, reply: %v", rf.me, rf.role, Any2String(args), Any2String(reply))
	}()

	reply.Term = rf.term
	reply.Success = false
	//拒绝旧leader请求
	if args.Term < rf.term {
		return
	}
	//发现一个更大的任期，转变成这个term的follower，leader、follower--> follower
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		//发现term大于等于自己的日志复制请求，则认其为主
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
	}
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.lastActiveTime = time.Now()

	reply.Success = true
}

// Leader HeartBeat
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
			if time.Now().Sub(rf.lastActiveTime) < HeatBeatTimeout {
				return
			}
			for i := 0; i < rf.nPeers; i++ {
				if rf.me == i {
					continue
				}
				//DPrintf("lastLogIndex: %v, logs: %v", lastLogIndex, mr.Any2String(rf.logs))
				argsI := &AppendEntriesArgs{
					LogType:  HeartBeatLogType,
					Term:     rf.term,
					LeaderId: rf.me,
				}

				go func(server int, args *AppendEntriesArgs) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
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
						return
					} else {
						rf.lastActiveTime = time.Now()
					}
				}(i, argsI)
			}
		}()
	}
}

// Follower Election
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 10)
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
			rf.timeoutInterval = randElectionTimeout()
			rf.votedFor = rf.me
			rf.term++
			lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
			rf.mu.Unlock()

			maxTerm, voteGranted := rf.becomeCandidate(lastLogIndex, lastLogTerm)
			rf.mu.Lock()
			//DPrintf("node[%d] get vote num[%d]", rf.me, totalVote)

			//在这过程中接收到更大term的请求，导致退化为follower
			if rf.role != Candidate {
				//DPrintf("node[%d] role[%v] failed to leader, voteGranted[%d], totalVote[%d]", rf.me, rf.role, voteGranted, totalVote)
				return
			}
			if maxTerm > rf.term {
				rf.role = Follower
				rf.term = maxTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
			} else if voteGranted > rf.nPeers/2 {
				rf.leaderId = rf.me
				rf.role = Leader
				rf.lastActiveTime = time.Now()
			}
			DPrintf("node[%d] role[%v] maxTerm[%d] voteGranted[%d] nPeers[%d]", rf.me, rf.role, maxTerm, voteGranted, rf.nPeers)
		}()
	}
}

func (rf *Raft) becomeCandidate(lastLogIndex, lastLogTerm int) (int, int) {

	voteChan := make(chan *RequestVoteReply, rf.nPeers-1)
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
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
				voteChan <- reply
			} else {
				voteChan <- nil
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
			if vote != nil {
				if vote.VoteGranted {
					voteGranted++
				}
				//出现更大term就退回follower
				if vote.Term > maxTerm {
					maxTerm = vote.Term
				}
			}
		}
		if voteGranted > rf.nPeers/2 || totalVote == rf.nPeers {
			return maxTerm, voteGranted
		}
	}
	return maxTerm, voteGranted
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		term:           None,
		votedFor:       RoleNone,
		role:           Follower,
		lastActiveTime: time.Now(),
		//lastHeartBeatTime: time.Now(),
		timeoutInterval: randElectionTimeout(),
		applyChan:       applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("starting new raft node, id[%d], lastActiveTime[%v], timeoutInterval[%d]", me, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds())

	rf.logs = make([]*LogEntry, 0)
	rf.logs = append(rf.logs, &LogEntry{
		LogTerm: 0,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()

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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}
