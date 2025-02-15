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
	HEARTBEAT_TIMEOUT    = 100
	MIN_ELECTION_TIMEOUT = 150
	MAX_ELECTION_TIMEOUT = 300
	RPC_TIMEOUT          = time.Second
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
	VotedFor    int // maybe voting for ?
	Logs        []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on leaders
	NextIndex     []int
	MatchIndex    []int
	lastSendIndex int

	state         int
	applyChannel  chan ApplyMsg
	electionTimer *time.Timer
	leaderId      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
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

type AppendEntriesArgs struct {
	// Arguments:
	// 	term leader’s term
	// 	leaderId so follower can redirect clients
	// 	prevLogIndex index of log entry immediately preceding new ones
	// 	prevLogTerm term of prevLogIndex entry
	// 	entries[] log entries to store (empty for heartbeat; may send more than one for efficiency)
	// 	leaderCommit leader’s commitIndex
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Results:
	// 	term currentTerm, for leader to update itself
	// 	success true if follower contained entry matching
	// 	prevLogIndex and prevLogTerm
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	if args.Term > rf.CurrentTerm {
		rf.beFollower(args.Term, true)
	}
	lastTerm := 0
	if len(rf.Logs) > 0 {
		lastTerm = rf.Logs[len(rf.Logs)-1].Term
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && aUpToData(args.LastLogTerm, args.LastLogIndex, lastTerm, len(rf.Logs)) {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		// fmt.Println(rf.me, "vote for", args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		fmt.Println(rf.me, "reject append entries from", args.LeaderId)
		return
	}
	rf.beFollower(args.Term, true)
	rf.mu.Lock()
	rf.leaderId = args.LeaderId
	reply.Term = args.Term
	// 校验leader的日志
	lastTerm := 0
	if len(rf.Logs) > args.PrevLogIndex && args.PrevLogIndex >= 0 {
		lastTerm = rf.Logs[args.PrevLogIndex].Term
	}
	reply.Success = len(rf.Logs) > args.PrevLogIndex && (args.PrevLogIndex == -1 || lastTerm == args.PrevLogTerm)
	// 维护日志
	if reply.Success {
		// fmt.Println(rf.me, "'s logs replicated successfully")
		if args.PrevLogIndex >= 0 {
			rf.Logs = rf.Logs[:args.PrevLogIndex+1]
		}
		rf.Logs = append(rf.Logs, args.Entries...)
		rf.CommitIndex = args.LeaderCommit
		// 不考虑回退状态机
		rf.LastApplied = min(rf.LastApplied, rf.CommitIndex)
		if rf.LastApplied < rf.CommitIndex {
			for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
				fmt.Println(rf.me, "apply", i+1)
				rf.applyChannel <- ApplyMsg{Index: i + 1, Command: rf.Logs[i].Command}
			}
			rf.LastApplied = rf.CommitIndex
		}
		rf.persist()
	}
	rf.leaderId = args.LeaderId
	// fmt.Println(rf.me, "receive append entries from", args.LeaderId)
	rf.mu.Unlock()
}

func aUpToData(aTerm, aIndex, bTerm, bIndex int) bool {
	if aTerm != bTerm {
		return aTerm > bTerm
	}
	return aIndex >= bIndex
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	// fmt.Println(rf.me, "receive command")
	index := -1
	term := rf.CurrentTerm
	isLeader := rf.state == Leader
	if isLeader {
		fmt.Println(rf.me, "as the leader, append log", command)
		if len(rf.Logs) == 0 {
			rf.Logs = append(rf.Logs, LogEntry{Term: term, Command: command})
		} else if rf.Logs[len(rf.Logs)-1].Command != command {
			rf.Logs = append(rf.Logs, LogEntry{Term: term, Command: command})
		} else {
			fmt.Println(rf.me, "duplicated command")
		}
		fmt.Println("before append", rf.Logs)
		// rf.Logs = append(rf.Logs, LogEntry{Term: term, Command: command})
		fmt.Println("after append", rf.Logs)
		index = len(rf.Logs) - 1
		fmt.Println(rf.me, "append log", index, "now logs:", rf.Logs)
		rf.persist()
		// for {
		// 	time.Sleep(100 * time.Millisecond)
		// 	if rf.CommitIndex == index {
		// 		return index, term, isLeader
		// 	}
		// }
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// be Follower, update term and reset election timer if neccessary
func (rf *Raft) beFollower(newTerm int, resetElectionTimer bool) {
	rf.mu.Lock()
	rf.state = Follower
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.persist()
	if resetElectionTimer {
		rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
	}
	// fmt.Println(rf.me, "become follower at", newTerm)
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer(min, max int) {
	// if !rf.electionTimer.Stop() {
	// 	<-rf.electionTimer.C
	// }
	nextTimeout := min + rand.Intn(max-min)
	// fmt.Println(rf.me, "reset election timer to", nextTimeout)
	rf.electionTimer.Reset(time.Duration(nextTimeout) * time.Millisecond)
}

// be Leader
func (rf *Raft) beLeader() {
	rf.mu.Lock()
	fmt.Println(rf.me, "become leader at", rf.CurrentTerm)
	rf.state = Leader
	rf.VotedFor = -1
	rf.leaderId = rf.me
	rf.persist()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.NextIndex[i] = len(rf.Logs)
		rf.MatchIndex[i] = -1
	}
	rf.mu.Unlock()
}

// be Candidate, term++, vote for self
func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.state = Candidate
	rf.persist()
	fmt.Println(rf.me, "become candidate at", rf.CurrentTerm)
	rf.mu.Unlock()
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

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = []LogEntry{}
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.applyChannel = applyCh
	rf.leaderId = -1
	rf.electionTimer = time.NewTimer(MAX_ELECTION_TIMEOUT * time.Millisecond)
	rf.beFollower(0, true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tryElection()
	return rf
}

func (rf *Raft) tryElection() {
	// wait for rf.electionTimer
	for {
		<-rf.electionTimer.C
		// fmt.Println(rf.me, "try to become candidate")
		if rf.state == Leader {
			// Leader -> Leader, heart beating
			// fmt.Println(rf.me, "already leader, heart beat")
			rf.sendAppendEntries()
			continue
		}
		if rf.state == Follower {
			// Follower -> Candidate
			rf.beCandidate()
			voteResult := make(chan bool, len(rf.peers))
			voteCount := 0
			voteResult <- true // self vote
			for i := range rf.peers {
				if i == rf.me {
					// don't send request vote to self
					continue
				}
				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.Logs),
					LastLogTerm:  0,
				}
				if len(rf.Logs) > 0 {
					args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
				}
				reply := RequestVoteReply{}

				go func(dst int) {
					// fmt.Println(rf.me, "send request vote to", dst)
					if rf.sendRequestVote(dst, args, &reply) {
						// fmt.Println(rf.me, "receive request vote reply from", dst, "term", reply.Term, "vote granted", reply.VoteGranted)
						if reply.Term == rf.CurrentTerm {
							// 防止收到过期的投票
							voteResult <- reply.VoteGranted
						} else if reply.Term > rf.CurrentTerm {
							rf.beFollower(reply.Term, true)
							voteResult <- false
						}
					} else {
						// fmt.Println(rf.me, "send request vote to", dst, "failed")
						// TODO
						voteResult <- false
					}
				}(i)
			}

			if rf.state == Candidate {
				// fmt.Println(rf.me, "still candidate, counting votes")
				for range rf.peers {
					select {
					case voteGranted := <-voteResult:
						if voteGranted {
							voteCount++
							// fmt.Println(rf.me, "vote count", voteCount)
						}
					case <-time.After(RPC_TIMEOUT):
						// fmt.Println(rf.me, "request vote timeout")
						break
					}
				}
				if voteCount > len(rf.peers)/2 {
					// become leader
					// fmt.Println(rf.me, "should become leader")
					rf.beLeader()
					rf.sendAppendEntries()
				} else {
					// election failed
					// fmt.Println(rf.me, "election failed, try another round later")
					rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
					continue
				}

			}
		}
		if rf.state == Candidate {
			// Candidate -> Candidate
			rf.beCandidate()
			voteResult := make(chan bool, len(rf.peers))
			voteCount := 0
			voteResult <- true // self vote
			for i := range rf.peers {
				if i == rf.me {
					// don't send request vote to self
					continue
				}
				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.Logs),
					LastLogTerm:  0,
				}
				if len(rf.Logs) > 0 {
					args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
				}
				reply := RequestVoteReply{}

				go func(dst int) {
					// fmt.Println(rf.me, "send request vote to", dst)
					if rf.sendRequestVote(dst, args, &reply) {
						// fmt.Println(rf.me, "receive request vote reply from", dst, "term", reply.Term, "vote granted", reply.VoteGranted)
						if reply.Term == rf.CurrentTerm {
							// 防止收到过期的投票
							voteResult <- reply.VoteGranted
						} else if reply.Term > rf.CurrentTerm {
							rf.beFollower(reply.Term, true)
							voteResult <- false
						}
					} else {
						// fmt.Println(rf.me, "send request vote to", dst, "failed")
						// TODO
						voteResult <- false
					}
				}(i)
			}

			time.Sleep(100 * time.Millisecond)
			if rf.state == Candidate {
				// fmt.Println(rf.me, "still candidate, counting votes")
				for range rf.peers {
					select {
					case voteGranted := <-voteResult:
						if voteGranted {
							voteCount++
							// fmt.Println(rf.me, "vote count", voteCount)
						}
					case <-time.After(RPC_TIMEOUT):
						// fmt.Println(rf.me, "request vote timeout")
						break
					}
				}
				if voteCount > len(rf.peers)/2 {
					// become leader
					// fmt.Println(rf.me, "should become leader")
					rf.beLeader()
					rf.sendAppendEntries()
					continue
				} else {
					// election failed
					// fmt.Println(rf.me, "election failed, try another round later")
					rf.resetElectionTimer(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
					continue
				}

			}
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	// rf.mu.Lock()
	// updateEndIndex := len(rf.Logs)
	// isHeartBeat := updateEndIndex == rf.lastSendIndex
	// rf.lastSendIndex = updateEndIndex // ?
	// rf.mu.Unlock()
	appendResult := make(chan bool, len(rf.peers))
	fmt.Println(rf.me, "send append entries, self logs", rf.Logs)
	for i := range rf.peers {
		if i == rf.me {
			// don't send heart beat to self
			appendResult <- true
			continue
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		go func(dst int) {
			// fmt.Println(rf.me, "send heart beat to", dst)
			var entries []LogEntry
			// fmt.Println(rf.me, "leader info", rf.NextIndex[dst], len(rf.Logs))
			if rf.NextIndex[dst] == len(rf.Logs) {
				entries = nil
			} else {
				entries = rf.Logs[rf.NextIndex[dst]:]
			}
			predLogIndex := rf.NextIndex[dst] - 1
			predLogTerm := 0
			if predLogIndex >= 0 {
				predLogTerm = rf.Logs[predLogIndex].Term
			}
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: predLogIndex, // 认为我们已对这个日志（及其之前）达成了一致
				PrevLogTerm:  predLogTerm,  // 认为我们已对这个日志（及其之前）达成了一致
				Entries:      entries,      // 基于上两条假设所需要你附加的新日志
				LeaderCommit: rf.CommitIndex,
			}
			// fmt.Println(rf.me, "send heart beat to", dst, "with args", args)
			reply := AppendEntriesReply{}
			ok := rf.peers[dst].Call("Raft.AppendEntries", args, &reply)
			if ok {
				// fmt.Println(rf.me, "receive heartbeat from", dst, "reply", reply)
				appendResult <- reply.Success
				if reply.Term == rf.CurrentTerm && rf.state == Leader {
					rf.mu.Lock()
					if reply.Success && len(entries) > 0 {
						rf.NextIndex[dst] += len(entries)
						rf.MatchIndex[dst] = rf.NextIndex[dst] - 1
					}
					if !reply.Success {
						fmt.Println(rf.me, "send", args, "return failure with reply", reply)
						rf.NextIndex[dst]--
					}
					rf.mu.Unlock()
				} else {
					rf.beFollower(reply.Term, true)
				}
			} else {
				fmt.Println(rf.me, "failed to send heartbeat to", dst)
			}
		}(i)
	}

	time.Sleep(MIN_ELECTION_TIMEOUT / 2)
	rf.mu.Lock()
	for i := rf.CommitIndex + 1; i < len(rf.Logs); i++ {
		count := 1
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.MatchIndex[j] >= i {
				count++
			}
		}
		if count > (len(rf.peers)+1)/2 {
			fmt.Println(rf.me, "commit index", i+1)
			rf.applyChannel <- ApplyMsg{Index: i + 1, Command: rf.Logs[i].Command}
			rf.CommitIndex = i
		}
	}
	rf.mu.Unlock()
	// for range rf.peers {
	// 	select {
	// 	case success := <-appendResult:
	// 		if success {
	// 			// fmt.Println(rf.me, "append success")
	// 		} else {
	// 			// fmt.Println(rf.me, "append failed")
	// 		}
	// 	case <-time.After(RPC_TIMEOUT):
	// 		// fmt.Println(rf.me, "heartbeat timeout")
	// 		break
	// 	}
	// }
	rf.resetElectionTimer(MIN_ELECTION_TIMEOUT/2, MIN_ELECTION_TIMEOUT)
}
