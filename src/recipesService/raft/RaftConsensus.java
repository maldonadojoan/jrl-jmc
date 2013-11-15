/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This file is part of the practical assignment of Distributed Systems course.
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This code is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this code.  If not, see <http://www.gnu.org/licenses/>.
 */

package recipesService.raft;


import java.rmi.RemoteException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import communication.DSException;
import communication.rmi.RMIsd;
import recipesService.CookingRecipes;
import recipesService.activitySimulation.SimulationData;
import recipesService.communication.Host;
import recipesService.data.AddOperation;
import recipesService.data.Operation;
import recipesService.data.RemoveOperation;
import recipesService.raft.dataStructures.Index;
import recipesService.raft.dataStructures.LogEntry;
import recipesService.raft.dataStructures.PersistentState;
import recipesService.raftRPC.AppendEntriesResponse;
import recipesService.raftRPC.RequestVoteResponse;
import recipesService.test.client.RequestResponse;

/**
 * 
 * Raft Consensus
 * 
 * @author Joan-Manuel Marques
 * May 2013
 *
 */

public abstract class RaftConsensus extends CookingRecipes implements Raft{

	// current server
	private Host localHost;

	//
	// STATE
	//

	// raft persistent state state on all servers
	protected PersistentState persistentState;  

	// raft volatile state on all servers
	private int commitIndex; // index of highest log entry known to be committed (initialized to 0, increases monotonically) 
	private int lastApplied; // index of highest log entry applied to state machine (initialized to 0, increases monotonically) 

	// other 
	private RaftState state = RaftState.FOLLOWER;

	// Leader
	private String leader; 

	// Leader election
	private Timer electionTimeoutTimer; // timer for election timeout
	private long electionTimeout; // period of time that a follower receives no communication.
	
	/** TimerTask who notices that a leader has collapsed */
	private final TimerTask electionTimeoutTimerTask = new TimerTask() {
		
		@Override
		public void run() {
			// If we are not a leader, start an election.
			String serverId = getServerId();
			if ( !isLeader() ) { 
				// TODO Start election
				System.out.println("STARTING ELECTION ON HOST " + serverId);
				leaderElection(serverId);
			}
		}
	};

	private RMIsd communication; //RPC communication
	
	// If a timeout occurs, it assumes there is no viable leader.
	private Set<Host> receivedVotes; // contains hosts that have voted for this server as candidate in a leader election

	//
	// LEADER
	//

	// Volatile state on leaders
	private Index nextIndex; // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	private Index matchIndex; // for each server, index of highest log known to be replicated on server (initialized to 0, increases monotonically)

	// Heartbeats on leaders
	private Timer leaderHeartbeatTimeoutTimer;
	private long leaderHeartbeatTimeout;

	//
	// CLUSTER
	//

	// general
	private int numServers; // number of servers in a Raft cluster.
	// 5 is a typical number, which allows the system to tolerate two failures 
	// partner servers
	private List<Host> otherServers; // list of partner servers (localHost not included in the list)

	//
	// UTILS
	//

	static Random rnd = new Random();

	// =======================
	// === IMPLEMENTATION
	// =======================

	public RaftConsensus(long electionTimeout){ // electiontimeout is a parameter in config.properties file
		// set electionTimeout
		this.electionTimeout = electionTimeout;

		//set leaderHeartbeatTimeout
		this.leaderHeartbeatTimeout = electionTimeout / 3; //TODO: Cal revisar-ne el valor 
	}

	// sets localhost and other servers participating in the cluster
	protected void setServers(
			Host localHost,
			List<Host> otherServers
			){

		this.localHost = localHost; 

		// initialize persistent state  on all servers
		persistentState = new PersistentState();

		// set servers list
		this.otherServers = otherServers;
		numServers = otherServers.size() + 1;	
	}

	// connect
	public void connect(){
		/*
		 *  ACTIONS TO DO EACH TIME THE SERVER CONNECTS (i.e. when it starts or after a failure or disconnection)
		 */
		
		// Start the election timeout.
		electionTimeoutTimer = new Timer();
		electionTimeoutTimer.schedule(electionTimeoutTimerTask, getTimeoutDate());
	}

	/**
	 * Calculates the timestamp of the moment the leader will be assumed to be stale.
	 * 
	 * @return System.currentTimeMillis() + randomTimeout.
	 */
	private Date getTimeoutDate() {
		return new Date(System.currentTimeMillis() + (long) (rnd.nextFloat() * electionTimeout) + electionTimeout); 
		// random float between 0 and 1 * timeout + timeout means a value between [electionTimeout , electionTimeout * 2].
	}

	public void disconnect(){
		electionTimeoutTimer.cancel();
		if (isLeader()){
			leaderHeartbeatTimeoutTimer.cancel();
		}
	}

	/**
	 * @return true if the localhost server is leader of the cluster.
	 */
	protected boolean isLeader() {
		return localHost.getId().equals(leader);
	}


	//
	// LEADER ELECTION
	//

	/*
	 *  Leader election
	 */
	private void leaderElection(String serverId) {
	// Steps
		// 1-Increment current term
		long newTerm = this.persistentState.getCurrentTerm() +1;
		this.persistentState.setCurrentTerm(newTerm);
		// 2-Change to candidate state
		this.state = RaftState.CANDIDATE;
		// 3-Vote for self
		this.persistentState.setVotedFor(serverId);
		// 4-Send RequestVote RPC to all other servers
		try {
			this.requestVote(newTerm, serverId, this.persistentState.getLastLogIndex(), this.persistentState.getLastLogTerm());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 5. Retry until:
			// 5.1. Receive votes from majority of servers
			// 5.2. Receive RPC from a valid Leader
			// 5.3. Election timeout elapses (no one wins the election)
				// 5.3.1. Increment current term
				// 5.3.2. Start a new election
		
	}



	//
	// LOG REPLICATION
	//

	/*
	 *  Log replication
	 */

	/**
	 * Follower code on invokation of appendEntries from the leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat.
	 * @param term: leader's term
	 * @param leaderId: so follower can redirect clients
	 * @param prevLogIndex: index of log entry immediately preceding new ones
	 * @param prevLogTerm: term of prevLogIndex entry 
	 * @param entries: log entries to store (empty for heartbeat: may send more than one for efficiency) 
	 * @param leaderCommit: leader's commitIndex 
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update itself), success (true if follower contained entry matching prevLongIndex and prevLogTerm)
	 * @throws RemoteException
	 */
	private AppendEntriesResponse followerAppendEntries(long term, String leaderId,
			int prevLogIndex, long prevLogTerm, List<LogEntry> entries,
			int leaderCommit) {
		// If stale leader, notify him
		if ( term < persistentState.getCurrentTerm() ) {
			return new AppendEntriesResponse(persistentState.getCurrentTerm(), false);
		}

		// If not a stale leader, count from now the heartbeat.
		onHeartbeat();

		// If in a newer term, update my own to the newer.
		if ( term > persistentState.getCurrentTerm() ) {
			persistentState.setCurrentTerm(term);
		}
		
		// Here on, always current term. Try to append to log.
		// If correct log and term index...
		if ( prevLogIndex == persistentState.getLastLogIndex() && prevLogTerm == persistentState.getLastLogTerm() ) {

			// Store it to log.
			for ( LogEntry entry : entries ) {
				persistentState.appendEntry(entry);
			}

			// XXX JOSEP Commit log up to commitIndex.
			
			return new AppendEntriesResponse(term, true);
		}
		// If not correct, notify the leader that we need a previous log first.
		else {
			return new AppendEntriesResponse(term, false);
		}
	}
	
	/**
	 * leader code for serving a request to a user.
	 * 
	 * Invoked by a client to apply an operation to the recipes' service
	 * Operation may be: AddOpperation (to add a recipe) or RemoveOperation (to remove a recipe)
	 * @param Operation: operation to apply to recipes
	 * @return RequestResponse: the leaderId and a boolean that indicates if operation succeeded or not
	 * @throws RemoteException
	 */
	private RequestResponse leaderRequest(Operation operation) {
		// Step 1 -> store to my own log.
		persistentState.addEntry(operation);

		// The last index is the one we will need to get
		int indexCommitRequired = persistentState.getLastLogIndex();
		
		// Step 2 -> Start sending append entries to the rest of the cluster. There is already
		// A thread doing this, so we just have to wait for the thread to get to the current operation and break.
		// We have to be careful for:
		// * State changes (if we become a leader we won't commit any more data).
		// * ??
		do {
			if (state == RaftState.LEADER) {
				// If we have changed of state unsuccessful request.
				return new RequestResponse(getServerId(), false);		
			}
			
			// Commit index MUST be updated when persisted at threads.
			if ( indexCommitRequired >= commitIndex ) {
				break;
			}
			
//			Alternative way
//			if ( isIndexCommitted(indexCommitRequired) ) {
//				break;
//			}
			try {
				Thread.sleep(50);
			} catch ( Exception e ) {
				// nop, if we have been interrupted to stop this thread because of a state change, we will be stopped on the first if.
			}
		} while (true);
		
		// If we have reached this point, the entry has been committed.
		return new RequestResponse(getServerId(), true);
	}

	/**
	 * @param index the index of the log which we try to know if it is committed. 
	 * @return true if it has been committed.
	 */
	private boolean isIndexCommitted(int indexCommitRequired) {
		int requiredCount = 1 + (numServers / 2); // At least one more than a half.
		int count = 0;
		for ( Host h : otherServers ) {
			int otherIndex = matchIndex.getIndex(h.getId());
			
			// If the index is higher than the required one, increase count.
			if ( otherIndex >= indexCommitRequired ) {
				count ++;
			}
			// If already at half the cluster, it is committed.
			if ( count == requiredCount ) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Leader code on invokation of an appendEntries from another leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat
	 * @param term: leader's term
	 * @param leaderId: so follower can redirect clients
	 * @param prevLogIndex: index of log entry immediately preceding new ones
	 * @param prevLogTerm: term of prevLogIndex entry 
	 * @param entries: log entries to store (empty for heartbeat: may send more than one for efficiency) 
	 * @param leaderCommit: leader's commitIndex 
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update itself), success (true if follower contained entry matching prevLongIndex and prevLogTerm)
	 * @throws RemoteException
	 */
	private AppendEntriesResponse leaderAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {
		if ( persistentState.getCurrentTerm() == term ) {
			// This is impossible, and means that there is some error on the code.
			// This could only happen if:
			// 1) The leader sends a message to himself.
			// 2) There are two leaders for a given term.
			if ( leaderId.equals(getServerId()) ) {
				System.err.println("TWO LEADERS AT THE SAME TERM!");
			}
			return null;
		}
		
		else if ( persistentState.getCurrentTerm() < term ) {
			// Should become follower if ownterm < term.
			changeState(RaftState.FOLLOWER);
			leader = leaderId;
			
			// And return follower response to the new leader.
			return followerAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
		}
		
		else {
			// There is a node at the cluster believing he is the leader, but actually is not.
			return new AppendEntriesResponse(Math.max(term, persistentState.getCurrentTerm()), false);
		}
	}

	/**
	 * Candidate code on invokation of an appendEntries from a leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat
	 * @param term: leader's term
	 * @param leaderId: so follower can redirect clients
	 * @param prevLogIndex: index of log entry immediately preceding new ones
	 * @param prevLogTerm: term of prevLogIndex entry 
	 * @param entries: log entries to store (empty for heartbeat: may send more than one for efficiency) 
	 * @param leaderCommit: leader's commitIndex 
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update itself), success (true if follower contained entry matching prevLongIndex and prevLogTerm)
	 * @throws RemoteException
	 */
	private AppendEntriesResponse candidateAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {
		
		// If equal or bigger term than ours, become follower
		if ( term >= persistentState.getCurrentTerm() ) {
			// Should become follower if ownterm < term.
			changeState(RaftState.FOLLOWER);
			leader = leaderId;
			
			// And return follower response to the new leader.
			return followerAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
		}
		// If smaller log entry,  answer the stale leader accordingly. 
		else {
			// There is a node at the cluster believing he is the leader, but actually is not.
			return new AppendEntriesResponse(Math.max(term, persistentState.getCurrentTerm()), false);
		}
	}
	
	//
	// Heartbeat received
	//
	
	/**
	 * Executed once a heartbeat is received.
	 */
	private void onHeartbeat() {
		// Reset the timer to a new timeout date.
		if ( electionTimeoutTimerTask.cancel() ) {
			electionTimeoutTimer.schedule(electionTimeoutTimerTask, getTimeoutDate());
		}
	}
	
	//
	// State changes
	//
	
	/**
	 * Executed when a change of state occurs.
	 * @param state
	 */
	private void changeState(RaftState state) {
		// XXX JOSEP Auto-generated method stub
		
	}

	//
	// API 
	//

	@Override
	public RequestVoteResponse requestVote(long term, String candidateId,
			int lastLogIndex, long lastLogTerm) throws RemoteException {
		// TODO Auto-generated method stub
		String serverIdTmp;
		RequestVoteResponse voteResponse;
		for( int i = 0 ; i < this.otherServers.size() ; i++ ){
			Host hostTmp = this.otherServers.get(i);
			serverIdTmp = hostTmp.getId();
			try {
				voteResponse = communication.requestVote(serverIdTmp, term, candidateId, lastLogIndex, lastLogTerm);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("DEBUG 1");
				e.printStackTrace();
			}
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see recipesService.raft.Raft#appendEntries(long, java.lang.String, int, long, java.util.List, int)
	 */
	@Override
	public AppendEntriesResponse appendEntries(long term, String leaderId,
			int prevLogIndex, long prevLogTerm, List<LogEntry> entries,
			int leaderCommit) throws RemoteException {
		switch (state) {
		case FOLLOWER:
			return followerAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
		case CANDIDATE:
			return candidateAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
		case LEADER:
			return leaderAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
		}

		// Should never happen.
		return null;
	}

	@Override
	public RequestResponse Request(Operation operation) throws RemoteException {
		// If follower and candidate, redirect him to the last leader known, so it will retry.
		switch ( state ) {
		default:
		case FOLLOWER: 
		case CANDIDATE: // TODO WHERE SHOULD CANDIDATE REDIRECT?
			return new RequestResponse(leader, false);
		case LEADER:
			return leaderRequest(operation);
		}
	}

	//
	// Other methods
	//
	public String getServerId(){
		return localHost.getId();
	}

	public synchronized List<LogEntry> getLog(){
		return persistentState.getLog();
	}
}
