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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	private TimerTask electionTimeoutTimerTask;

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

	private ExecutorService executorQueue;

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

		// Create the executor queue.
		this.executorQueue = Executors.newCachedThreadPool();
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
		// Server starts always as FOLLOWER
		this.state = RaftState.FOLLOWER;
		communication = RMIsd.getInstance();

		restartElectionTimeout();
	}

	/**
	 * (re)starts the election timeout, so if we receive a heartbeat it won't be started until
	 * a new timeout has passed.
	 */
	private void restartElectionTimeout() {
		// If already created the election timeout timers, stop them.
		if ( electionTimeoutTimer != null ) {
			electionTimeoutTimer.cancel();
			electionTimeoutTimerTask.cancel();
		}


		// Start the election timeout.
		electionTimeoutTimer = new Timer();
		electionTimeoutTimerTask = new TimerTask() {

			@Override
			public void run() {
				// If we are not a leader, start an election.
				if ( !isLeader() ) { 
					System.out.println("STARTING ELECTION ON HOST " + getServerId());
					leaderElection();
				}
			}
		};
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
	private void leaderElection() {
		// Steps
		///////////// RESETEJAR electionTimeout?//////////////////////////////////////////////////////////////////////////
		// 1-Increment current term
		persistentState.nextTerm();

		// 2-Change to candidate state
		this.state = RaftState.CANDIDATE;

		// 3-Vote for self
		this.persistentState.setVotedFor(getServerId());
		// Clear list of received votes
		this.receivedVotes = new HashSet<Host>();
		System.out.println("\nCANDIDATE HOST: " + localHost.toString()); /////////////////////////////////////////////////////////DEBUG
		this.receivedVotes.add(localHost);
		System.out.println("\nRECEIVED VOTES: " + Integer.toString(receivedVotes.size()) + " OTHER SERVERS: " + Integer.toString(otherServers.size())); ///////////////DEBUG
		// 4-Send RequestVote RPC to all other servers
		// 5. Retry until:  I think that this could be done by:
		// 5.1. Receive votes from majority of servers Everytime a thread of the executor obtains a vote, try to see if majority is obtained.
		//while (!(receivedVotes.size() > otherServers.size()/2+1)){
		System.out.println("\nASKING FOR VOTES"); /////////////////////////////////////////////////////////DEBUG
		try {
			//This method should start threads in an executor. For each server, a thread retrying and trying to get its vote.
			this.requestVote(persistentState.getCurrentTerm(), getServerId(), this.persistentState.getLastLogIndex(), this.persistentState.getLastLogTerm());
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		//}
		// 5.2. Receive RPC from a valid Leader onAppendEntry, increase term. the threads of the executor must check if the term is correct.
		// 5.3. Election timeout elapses (no one wins the election) This should be done with the other timer and a timer task,
		//  similar to the heartbeat one. Again, increase term.
		// 5.3.1. Increment current term // The timer would make this actions.
		// 5.3.2. Start a new election


		// Example thread usage:
		//		final int electionTerm = 5; //Example
		//		Object guard = null;
		//		doInBackground(new RaftGuardedRunnable(guard, 5) {
		//			
		//			@Override
		//			public boolean doRun() {
		//				// This part is executed in a synchronized environment, and retried 5 times if it has failed.
		//				return communication.requestVote(destination, term, id, lastLogIndex, lastLogTerm);
		//			}
		//			
		//			@Override
		//			public boolean canRun() {
		//				// when it becomes false, the runnable will cease to try to execute.
		//				return persistentState.getCurrentTerm() == electionTerm;
		//			}
		//		});
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
	 * DELETE : persistent state can do this, just keep it until we are sure we don't need it.
	 * @param index the index of the log which we try to know if it is committed. 
	 * @return true if it has been committed.
	 *
//	private boolean isIndexCommitted(int indexCommitRequired) {
//		int requiredCount = 1 + (numServers / 2); // At least one more than a half.
//		int count = 0;
//		for ( Host h : otherServers ) {
//			int otherIndex = matchIndex.getIndex(h.getId());
//			
//			// If the index is higher than the required one, increase count.
//			if ( otherIndex >= indexCommitRequired ) {
//				count ++;
//			}
//			// If already at half the cluster, it is committed.
//			if ( count == requiredCount ) {
//				return true;
//			}
//		}
//		return false;
//	}
	 */

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
		restartElectionTimeout();
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
		/*String serverIdTmp;
		RequestVoteResponse voteResponse = new RequestVoteResponse (term, false) ;
		System.out.println("\nOTHER SERVERS SIZE: " + Integer.toString(this.otherServers.size())); /////////////////////////////////////DEBUG
		for( int i = 0 ; i < this.otherServers.size() ; i++ ){
			Host hostTmp = this.otherServers.get(i);
			serverIdTmp = hostTmp.getId();
			System.out.println("\nELECTOR HOST NUM: " + Integer.toString(i) + " HOST: " + hostTmp.toString()); ///////////////////////////////////////////////////////////////DEBUG
			try {
				// Send RequestVote RPC to every server
				voteResponse = communication.requestVote(serverIdTmp, term, candidateId, lastLogIndex, lastLogTerm);
				System.out.println("\nVOTE RESPONSE: " + voteResponse.toString()); /////////////////////////////////////////////////////DEBUG
				// If the server gives its vote, we add it to out set of votes
				if (voteResponse.isVoteGranted()){
					System.out.println("\nVOTE RECEIVED"); /////////////////////////////////////////////////////////////////////////////DEBUG
					this.receivedVotes.add(hostTmp);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		final long termTmp = term;
		final String candidateIdTmp = candidateId;
		final int lastLogIndexTmp = lastLogIndex;
		final long lastLogTermTmp = lastLogTerm;
		final long electionTerm = persistentState.getCurrentTerm(); // XXX Joan, this had to be the current election term. Previous value was 5 hardcoded.
		Object guard = new Object();
		doInBackground(new RaftGuardedRunnable(guard, 5) {

			@Override public boolean doRun() {  
				String serverIdTmp;
				RequestVoteResponse voteResponse = new RequestVoteResponse (termTmp, false) ;
				for( int i = 0 ; i < otherServers.size() ; i++ ){
					Host hostTmp = otherServers.get(i);
					serverIdTmp = hostTmp.getId();
					System.out.println("\nELECTOR HOST NUM: " + Integer.toString(i) + " HOST: " + hostTmp.toString()); /////////////////////////DEBUG
					try{ 
						// Send RequestVote RPC to every server
						voteResponse = communication.requestVote(serverIdTmp, termTmp, candidateIdTmp, lastLogIndexTmp, lastLogTermTmp);
						System.out.println("\nVOTE RESPONSE: " + voteResponse.toString()); /////////////////////////////////////////////////////DEBUG
						// If the server gives its vote, we add it to out set of votes
						if (voteResponse.isVoteGranted()){
							System.out.println("\nVOTE RECEIVED"); /////////////////////////////////////////////////////////////////////////////DEBUG
							receivedVotes.add(hostTmp);
						}
					} 
					catch(Exception e){
						e.printStackTrace();
					}  
				}
				return voteResponse.isVoteGranted();
			}


			@Override
			public boolean canRun() {
				// when it becomes false, the runnable will cease to try to execute.
				return persistentState.getCurrentTerm() == electionTerm;
			}
		});

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

	// thread-safe runnables
	/**
	 * This class implements a runnable guarded with a given object, and with a given number of retries. 
	 * @author josep
	 */
	public static abstract class RaftGuardedRunnable implements Runnable {

		private Object guard;
		private int retries;

		/**
		 * @param guard the guard object if any.
		 * @param retries the number of retries to perform.
		 */
		public RaftGuardedRunnable(Object guard, int retries) {
			this.guard = guard;
			this.retries = retries;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public final void run() {
			// Synchronized depending on the guard we have received
			synchronized (guard) {
				synchronizedRun();
			}
		}

		/**
		 * Performs the synchronizedRun.
		 */
		private void synchronizedRun() {
			// If the thread can be ran (no change of states have occured in the meantime for example).
			if ( canRun() ) {
				// If the run has failed...
				boolean success = false;
				try {
					success = doRun();
				}
				catch ( Exception e){ }
				
				if ( !success ) {
					// If still have retries...
					if ( retries -- > 0 ) {
						System.out.println("Retrying RaftGuardedRunnable.");
						// Retry.
						synchronizedRun();
					} else {
						System.out.println("RaftGuardedRunnable has expired its retries.");
					}
				} else {
					System.out.println("RaftGuardedRunnable ran successfully.");
				}
			} else {
				System.out.println("Can't run RaftGuardedRunnable.");
			}
		}

		/**
		 * Executes the thread work.
		 * @return true if it succeeded. False otherwise.
		 */
		public abstract boolean doRun();

		/**
		 * @return true if the runnable can be run, false if it should be cancelled.
		 */
		public abstract boolean canRun();
	}

	public void doInBackground ( RaftGuardedRunnable runnable ) {
		// one call for each function that must be run in a separated thread
		executorQueue.execute(runnable);
	}
}
