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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

	// For 2D:
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

	lastActiveTime  time.Time
	timeoutInterval time.Duration

	term     int
	role     MemberType
	votedFor int
	leaderId int

	logs []*LogEntry

	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type MemberType int

const (
	Leader    MemberType = 1
	Follower  MemberType = 2
	Candidate MemberType = 3
	RoleNone  int        = 4
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.term, rf.role == Leader
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetLastTermAndIndex() (int, int) {
	LastIndex := len(rf.logs) - 1
	LastTerm := rf.logs[LastIndex].Term
	return LastIndex, LastTerm
}

const (
	HeartBeatTimeOut = 50 * time.Millisecond
	ElectionTimeOut  = 150 * time.Millisecond
)

func randElectionTime() time.Duration {
	return ElectionTimeOut + time.Duration(rand.Uint32())%ElectionTimeOut
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 实际上相当于是：Candidate把args发给指定Follower,Follower 实际调用 RequestVote返回reply,所以这里应该写的是该怎么Reply
	// reply流程:(1)检查 投票约束+重复投票 (2)更新自己的信息 (3)确认投票

	// Follower rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false
	// 投票约束1
	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
	}
	// 投票约束2
	lastLogIndex, lastLogTerm := rf.GetLastTermAndIndex()
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	// 避免重复选票: 当没投过票/新任期/已经给该Candidate投票时 才投票
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		rf.term = args.Term
		rf.role = Follower
		rf.leaderId = args.CandidateId
		rf.votedFor = args.CandidateId
		rf.lastActiveTime = time.Now()
		rf.timeoutInterval = randElectionTime()
		reply.VoteGranted = true
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Follower rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
	}

	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.lastActiveTime = time.Now()
	// 我觉得每次心跳都要重置选举时间，因为收到心跳说明已经有leader了
	rf.timeoutInterval = randElectionTime()

	reply.Success = true
}

// // example code to send a RequestVote RPC to a server.
// // server is the index of the target server in rf.peers[].
// // expects RPC arguments in args.
// // fills in *reply with RPC reply, so caller should
// // pass &reply.
// // the types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // look at the comments in ../labrpc/labrpc.go for more details.
// //
// // if you're having trouble getting RPC to work, check that you've
// // capitalized all field names in structs passed over RPC, and
// // that the caller passes the address of the reply struct with &, not
// // the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		func() { // 一定要加这个 func() 不然defer_Unlock将在for_killed之后才释放, 而且return会直接结束整个loop
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Leader {
				return
			}
			if time.Now().Sub(rf.lastActiveTime) < HeartBeatTimeOut {
				return
			}
			for i := 0; i < rf.nPeers; i++ {
				if rf.me == i {
					continue
				}
				argsI := &AppendEntriesArgs{
					Term:     rf.term,
					LeaderId: rf.me,
				}
				// 这里需要起一个协程来并行地广播心跳
				go func(server int, args *AppendEntriesArgs) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, argsI, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.term {
						rf.role = Follower
						rf.term = reply.Term
						rf.votedFor = RoleNone
						rf.leaderId = RoleNone
						return
					} else {
						rf.lastActiveTime = time.Now()
					}
				}(i, argsI)

			}
		}()
	}
}

func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		// 很容易没加锁
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				return
			}
			if time.Now().Sub(rf.lastActiveTime) < rf.timeoutInterval {
				return
			}

			if rf.role == Follower {
				rf.role = Candidate
			}
			rf.term++
			rf.votedFor = rf.me
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTime()
			lastLogIndex, lastLogTerm := rf.GetLastTermAndIndex()

			rf.mu.Unlock()
			maxTerm, VoteNum := rf.becomeCandidate(lastLogIndex, lastLogTerm)
			rf.mu.Lock()

			if rf.role != Candidate {
				return
			}

			if maxTerm > rf.term {
				rf.term = maxTerm
				rf.role = Follower
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
			} else if VoteNum > rf.nPeers/2 {
				rf.role = Leader
				rf.leaderId = rf.me
				rf.lastActiveTime = time.Now()
			}
		}()

	}
}
func (rf *Raft) becomeCandidate(lastLogIndex, lastLogTerm int) (int, int) {
	voteChan := make(chan *RequestVoteReply, rf.nPeers-1) // -1是因为自己已经给自己投票了
	argsI := &RequestVoteArgs{
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
			ok := rf.sendRequestVote(server, argsI, reply)
			if ok {
				voteChan <- reply
			} else {
				voteChan <- nil
			}
		}(i, argsI)
	}

	maxTerm := rf.term
	VoteNum := 1
	TotalVote := 1

	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case reply := <-voteChan:
			TotalVote++
			if reply != nil {
				if reply.Term > maxTerm {
					maxTerm = reply.Term
				}
				if reply.VoteGranted {
					VoteNum++
				}
			}
		}
		if VoteNum > rf.nPeers/2 || TotalVote == rf.nPeers {
			return maxTerm, VoteNum
		}
	}
	return maxTerm, VoteNum

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:              sync.Mutex{},
		peers:           peers,
		persister:       persister,
		me:              me,
		dead:            -1,
		nPeers:          len(peers),
		lastActiveTime:  time.Now(),
		timeoutInterval: randElectionTime(),
		term:            0,
		role:            Follower,
		votedFor:        RoleNone,
		leaderId:        RoleNone,
		applyChan:       applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("starting new raft node, id[%d], lastActiveTime[%v], timeoutInterval[%d]", me, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds())

	rf.logs = make([]*LogEntry, 0)
	rf.logs = append(rf.logs, &LogEntry{
		Term: 0,
	})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()

	return rf
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
