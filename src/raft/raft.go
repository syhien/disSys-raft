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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// 枚举状态
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// 参数常量
const (
	MIN_ELECTION_TIMEOUT = 150
	MAX_ELECTION_TIMEOUT = 300
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	// state          int // 好像不需要区分
	leaderId     int
	applyChannel chan ApplyMsg
	unionTimer   *time.Timer
}

// reset the timer
func (rf *Raft) resetElectionTimer(isHeartbeat bool) {
	nextTimeout := time.Duration(MIN_ELECTION_TIMEOUT+rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)) * time.Millisecond
	if isHeartbeat {
		nextTimeout = 50 * time.Millisecond
	}
	rf.unionTimer.Reset(nextTimeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.leaderId == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.CurrentTerm)
	// e.Encode(rf.VotedFor)
	// e.Encode(rf.Logs)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.CurrentTerm)
	// d.Decode(&rf.VotedFor)
	// d.Decode(&rf.Logs)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	// Arguments:
	// 	term candidate’s term
	// 	candidateId candidate requesting vote
	// 	lastLogIndex index of candidate’s last log entry (§5.4)
	// 	lastLogTerm term of candidate’s last log entry (§5.4)
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	// Results:
	// 	term currentTerm, for candidate to update itself
	// 	voteGranted true means candidate received vote
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	fmt.Println(rf.me, "receive request vote from", args.CandidateId)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term > rf.CurrentTerm {
		// 无论是否投票，都要更新term
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		if rf.leaderId == rf.me {
			// 遇到term更高的，退位
			rf.leaderId = -1
		}
	}

	if rf.VotedFor == args.CandidateId {
		// 可能的冗余
		reply.VoteGranted = true
	} else if rf.VotedFor == -1 {
		lastIndex := len(rf.Logs) - 1
		reply.VoteGranted = args.LastLogTerm > rf.Logs[lastIndex].Term ||
			(args.LastLogTerm == rf.Logs[lastIndex].Term && args.LastLogIndex >= lastIndex)
	}
	if reply.VoteGranted {
		rf.VotedFor = args.CandidateId
		rf.resetElectionTimer(false)
	}
	fmt.Println(rf.me, "vote", reply.VoteGranted)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote() bool {
	// be candidate
	// 其实不需要显式转换（？
	rf.mu.Lock()
	rf.leaderId = -1 // 必须丢弃旧leader，不再是follower
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.resetElectionTimer(false)
	rf.mu.Unlock()

	fmt.Println(rf.me, "send request vote at", rf.CurrentTerm)

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Logs) - 1,
		LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
	}

	voteResult := make(chan bool, len(rf.peers))
	voteResult <- true // self

	for i := range rf.peers {
		if i == rf.me { // self
			continue
		}
		go func(dst int) {
			reply := RequestVoteReply{
				Term:        rf.CurrentTerm,
				VoteGranted: false,
			}
			if rf.leaderId != -1 || rf.CurrentTerm > args.Term { // 已经是leader或者是旧的选举
				voteResult <- false
				return
			} else if rf.peers[dst].Call("Raft.RequestVote", args, &reply) {
				fmt.Println(rf.me, "receive vote from", dst, reply.VoteGranted)
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					// 选举失败变回follower
					rf.CurrentTerm = reply.Term
					rf.VotedFor = -1
					// 不修改leaderId 可能正在给人投票
				}
				rf.mu.Unlock()
			}
			voteResult <- reply.VoteGranted
		}(i)
	}

	voteCount := 0
	for range rf.peers {
		select {
		case granted := <-voteResult:
			if granted {
				voteCount++
				fmt.Println(rf.me, "vote count", voteCount)
			}
			if rf.leaderId != -1 || voteCount > len(rf.peers)/2 {
				break
			}
		case <-time.After(100 * time.Millisecond):
			break
		}
	}
	return voteCount > len(rf.peers)/2
}

// ApplyEntry Struct, as RPC parameter
type ApplyEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// ApplyEntryReply Struct, as RPC return
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args ApplyEntriesArgs, reply *AppendEntriesReply) {
	rf.resetElectionTimer(false)
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm // 无条件回复

	if args.Term < rf.CurrentTerm || len(rf.Logs) <= args.PrevLogIndex || rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Success = true
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		if rf.leaderId == rf.me {
			// 遇到term更高的，退位
			rf.leaderId = -1
		}
		rf.leaderId = args.LeaderId
	}
	if args.Entries != nil {
		entryIndex := 0
		for i := args.PrevLogIndex + 1; i < len(rf.Logs) && entryIndex < len(args.Entries); i++ {
			rf.Logs[i] = args.Entries[entryIndex]
			entryIndex++
		}
		for entryIndex < len(args.Entries) {
			rf.Logs = append(rf.Logs, args.Entries[entryIndex])
			entryIndex++
		}
	}
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Logs)-1)
		go rf.apply(rf.CommitIndex)
	}
	rf.mu.Unlock()
}

// 同步到updateEnd
func (rf *Raft) sendAppendEntries(updateEnd int) {
	rf.resetElectionTimer(true)

	appendResult := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			appendResult <- true
			rf.resetElectionTimer(false) // 长一点不然莫名被打断
			continue
		}
		go func(dst int, appendEnd int) {
			reply := AppendEntriesReply{
				Term:    -1,
				Success: false,
			}
			for {
				rf.mu.Lock()
				var entries []LogEntry = nil // 必须显式指定为nil
				if appendEnd != -1 {
					if rf.leaderId != rf.me || rf.NextIndex[dst] >= appendEnd {
						// 丧权或已成为过时消息
						rf.mu.Unlock()
						break
					}
					entries = rf.Logs[rf.NextIndex[dst]:appendEnd]
				}
				prevLogIndex := rf.NextIndex[dst] - 1
				args := ApplyEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.Logs[prevLogIndex].Term,
					Entries:      entries,
					LeaderCommit: rf.CommitIndex,
				}
				rf.mu.Unlock()
				if !rf.peers[dst].Call("Raft.AppendEntries", args, &reply) {
					// TODO
					break
				}
				if reply.Term > rf.CurrentTerm || rf.leaderId != rf.me {
					// 丧权或已成为过时消息
					rf.mu.Lock()
					rf.CurrentTerm = reply.Term
					rf.leaderId = -1
					rf.VotedFor = -1
					rf.resetElectionTimer(false)
					rf.mu.Unlock()
					break
				}
				if reply.Success {
					if appendEnd != -1 {
						rf.mu.Lock()
						if rf.NextIndex[dst] < appendEnd-1 { // 会出现被别的进程更新到后面的情况吗
							rf.NextIndex[dst] = appendEnd - 1
							rf.MatchIndex[dst] = appendEnd - 1
						}
						rf.mu.Unlock()
					}
					break
				} else {
					rf.mu.Lock()
					appendEnd = len(rf.Logs)
					if rf.NextIndex[dst] > 1 {
						rf.NextIndex[dst] -= 1
					}
					rf.mu.Unlock()
				}
			}
			appendResult <- reply.Success
		}(i, updateEnd)
	}

	successCount := 0
	successCount += 1 // self
	for i := range rf.peers {
		select {
		case success := <-appendResult:
			if updateEnd == -1 && i > 1 {
				// 是heartbeat而且有人收到过了不用看后面了
				return
			}
			if updateEnd != -1 && success {
				successCount += 1
				if successCount > len(rf.peers)/2 {
					rf.mu.Lock()
					if updateEnd-1 > rf.CommitIndex {
						rf.CommitIndex = updateEnd - 1
						go rf.apply(rf.CommitIndex)
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

func (rf *Raft) apply(commitIndex int) {
	rf.mu.Lock()
	for rf.LastApplied < commitIndex {
		rf.LastApplied += 1
		rf.applyChannel <- ApplyMsg{
			Index:   rf.LastApplied,
			Command: rf.Logs[rf.LastApplied].Command,
		}
	}
	rf.mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = []LogEntry{{Term: -1, Command: nil}}
	fmt.Println("init logs", rf.Logs)

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.leaderId = -1
	rf.applyChannel = applyCh
	rf.unionTimer = time.NewTimer(time.Second)
	rf.resetElectionTimer(false)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			if rf.leaderId == rf.me {
				select {
				case <-rf.unionTimer.C: // send heartbeat
					go rf.sendAppendEntries(-1) // just heartbeat
				case <-time.After(10 * time.Millisecond):
					// do nothing
				}
			} else {
				select {
				case <-rf.unionTimer.C: // send request vote
					electionResult := rf.sendRequestVote()
					fmt.Println(rf.me, "election result", electionResult)
					if electionResult && rf.leaderId == -1 {
						rf.mu.Lock()
						fmt.Println(rf.me, "become leader at", rf.CurrentTerm)
						rf.leaderId = rf.me
						for i := range rf.peers {
							rf.NextIndex[i] = len(rf.Logs)
							rf.MatchIndex[i] = 0
						}
						rf.mu.Unlock()
					}
				case <-time.After(10 * time.Millisecond):
					// do nothing
				}
			}
		}
	}()

	return rf
}
