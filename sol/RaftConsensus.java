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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import recipesService.CookingRecipes;
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

import communication.rmi.RMIsd;

/**
 * 
 * Raft Consensus
 * 
 * @author Joan-Manuel Marques May 2013
 * 
 */

public abstract class RaftConsensus extends CookingRecipes implements Raft {

	// The date formatter for output
	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:MM:ss.SSS");

	// The guard object
	private static final Object guard = new Object();

	// Logging constants
	private static final int VERBOSE = 0;
	private static final int DEBUG = 1;
	private static final int INFO = 2;
	private static final int WARN = 3;
	private static final int ERROR = 4;
	private static final int DEBUG_LEVEL = WARN; // Everything from DEBUG_LEVEL to above will be logged.

	// current server
	private Host localHost;

	//
	// STATE
	//

	// raft persistent state state on all servers
	protected PersistentState persistentState;

	// raft volatile state on all servers
	private int commitIndex = 0; // index of highest log entry known to be

	// committed (initialized to 0, increases monotonically)
	private int lastApplied = 0; // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// other
	private RaftState state = RaftState.FOLLOWER;

	// Leader
	private String leader;

	// RPC communication
	private RMIsd communication = RMIsd.getInstance();

	// Leader election
	private long electionTimeout; // period of time that a follower receives no
	// communication.

	// If a timeout occurs, it assumes there is no viable leader.
	private Set<Host> receivedVotes; // contains hosts that have voted for this
	// server as candidate in a leader election

	//
	// LEADER
	//

	// Volatile state on leaders
	private CustomIndex nextIndex; // for each server, index of the next log entry to
	// send to that server (initialized to leader
	// last log index + 1)
	private CustomIndex matchIndex; // for each server, index of highest log known to
	// be replicated on server (initialized to 0,
	// increases monotonically)

	// Heartbeats on leaders
	private long leaderHeartbeatTimeout;

	//
	// CLUSTER
	//

	// general
	private int numServers; // number of servers in a Raft cluster.

	// 5 is a typical number, which allows the system to tolerate two failures
	// partner servers
	private List<Host> otherServers; // list of partner servers (localHost not
	// included in the list)

	// The number of votes required to become the leader (i.e. majority).
	private int clusterMajority;

	// If true we are currently connected
	private AtomicBoolean connected = new AtomicBoolean(false);

	// Threads created by the process
	private final ArrayList<VoteRequesterThread> voteRequesters = new ArrayList<VoteRequesterThread>();

	private final ArrayList<HeartBeatSenderThread> heartbeaters = new ArrayList<HeartBeatSenderThread>();

	private final CommitterThread committer = new CommitterThread();

	private LeaderWatcherThread leaderWatcher;

	//
	// UTILS
	//

	static Random rnd = new Random();

	// =======================
	// === IMPLEMENTATION
	// =======================

	public RaftConsensus(long electionTimeout) { 
		// electiontimeout is a parameter in config.properties file
		this.electionTimeout = electionTimeout;

		// set leaderHeartbeatTimeout
		this.leaderHeartbeatTimeout = electionTimeout / 3; // UOCTODO: Cal revisar-ne el valor
	}

	// sets localhost and other servers participating in the cluster
	protected void setServers(Host localHost, List<Host> otherServers) {

		this.localHost = localHost;

		// initialize persistent state on all servers
		persistentState = new PersistentState();

		// set servers list
		this.otherServers = otherServers;
		numServers = otherServers.size() + 1;
		// Holds the number of required votes.
		clusterMajority = (numServers / 2) + 1;
	}

	// connect
	public void connect() {
		/*
		 * ACTIONS TO DO EACH TIME THE SERVER CONNECTS (i.e. when it starts or
		 * after a failure or disconnection)
		 */
		// Server starts always as FOLLOWER
		log("Connected", INFO);

		if ( leaderWatcher == null ) {
			leaderWatcher = new LeaderWatcherThread(electionTimeout);
			// Start committer thread.
			committer.start();
			leaderWatcher.start();
		}


		if ( matchIndex == null ) {
			matchIndex = new CustomIndex(otherServers, 0);
		}

		synchronized (guard) {
			changeState(RaftState.FOLLOWER);
			setLeader(null);

			connected.set(true);
		}
	}

	/**
	 * Called on a disconnection of the server.
	 */
	public void disconnect() {
		log("<<<<<<<<<<<<< Disconnected at term "
				+ persistentState.getCurrentTerm() + " with leader " + leader
				+ " >>>>>>>>>>>", ERROR);

		// Stop all threads.
		synchronized (guard) {
			connected.set(false);

			// Stop vote requesters.
			for (RaftThread t : voteRequesters) {
				t.stopThread();
			}

			// Stop heartbeaters.
			for (RaftThread t : heartbeaters) {
				t.stopThread();
			}

			// Wait threads:

			try {
				for (RaftThread t : voteRequesters) {
					t.join();
				}
				for (RaftThread t : heartbeaters) {
					t.join();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @return true if the localhost server is leader of the cluster.
	 */
	protected boolean isLeader() {
		return (state == RaftState.LEADER);
	}

	//
	// LEADER ELECTION
	//

	/*
	 * Leader election
	 */
	private void startElection() {
		// Steps
		synchronized ( guard ) {
			// 2-Change to candidate state
			if (state == RaftState.CANDIDATE) {
				log(">>>>>>>> The election ended without choosing a leader, therefore, it will be restarted <<<<<<<<<< ", INFO);
			} else {
				log(">>>>>>>> Starting election on host " + getServerId(), INFO);
			}

			// 1-Increment current term
			persistentState.nextTerm();
			long term = persistentState.getCurrentTerm();
			log("Incrementing my term to " + term, INFO);

			// 2 - Clear list of received votes
			this.receivedVotes = new HashSet<Host>();

			// 3 - Change state
			changeState(RaftState.CANDIDATE);
			setLeader(null);

			// 4 - Vote for self
			this.persistentState.setVotedFor(getServerId());
			this.receivedVotes.add(localHost);


			// Stop previous vote requesters
			for ( RaftThread t : voteRequesters ) {
				t.stopThread();
			}
			voteRequesters.clear();

			// Start voteRequesters.
			for (Host h : otherServers) {
				VoteRequesterThread t = new VoteRequesterThread(h, persistentState.getCurrentTerm());
				voteRequesters.add(t);
				t.start();
			}
		}
	}

	//
	// LOG REPLICATION
	//

	/*
	 * Log replication
	 */

	/**
	 * Follower code on invokation of appendEntries from the leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat.
	 * 
	 * @param term : leader's term
	 * @param leaderId : so follower can redirect clients
	 * @param prevLogIndex : index of log entry immediately preceding new ones
	 * @param prevLogTerm : term of prevLogIndex entry
	 * @param entries : log entries to store
	 * @param leaderCommit : leader's commitIndex
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update itself), 
	 * 	success (true if follower contained entry matching prevLongIndex and prevLogTerm)
	 */
	private AppendEntriesResponse followerAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {
		synchronized (guard) {
			// From raft pdf:
			// Receiver implementation:
			// 1. Reply false if term < currentTerm (5.1)
			if ( term < persistentState.getCurrentTerm()) {
				return new AppendEntriesResponse(persistentState.getCurrentTerm(), false);
			}


			// If leader with higher term, update ourselves.
			else if (term > persistentState.getCurrentTerm()) {
				log("Received append entry with newer term.", INFO);
				synchronized (guard) {
					persistentState.setCurrentTerm(term);
					setLeader(leaderId);
				}
			}

			// Same term, with different leader update it.
			else if (term == persistentState.getCurrentTerm()) {
				log("Received append entries for my term. Updating leader if needed", DEBUG);
				synchronized (guard) {
					setLeader(leaderId);
				}
			}

			// If not a stale leader, count from now the heartbeat.
			onHeartbeat();

			// 2. Reply false if log doesn't contain an entry at prevLogIndex
			// whose term matches prevLogTerm (5.3)
			if ( persistentState.getTerm(prevLogIndex) != prevLogTerm ) {
				// 3. If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				// follow it (5.3)
				if ( prevLogIndex > 0 ) {
					log("Deleting invalid entries from log.", WARN);
					persistentState.deleteEntries(prevLogIndex);
				}
				log("Rejecting append entry because term " + prevLogTerm + "!=" + persistentState.getTerm(prevLogIndex), ERROR);
				return new AppendEntriesResponse(persistentState.getCurrentTerm(), false);
			}

			// 4. Append any new entries not already in the log
			for ( LogEntry e : entries ) {
				persistentState.appendEntry(e);
				log("Appended entry " + e + " at position " + persistentState.getLastLogIndex(), WARN);
			}

			// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
			if ( Math.min(leaderCommit, persistentState.getLastLogIndex()) > commitIndex ) {
				commitIndex = Math.min(leaderCommit, persistentState.getLastLogIndex());
				log("Commit index updated to " + commitIndex + " from leader. Start committing data." , WARN);
				// Start committer thread.
				committer.awakeThread();
			}

			// Ok to leader.
			return new AppendEntriesResponse(persistentState.getCurrentTerm(), true);
		}
	}

	/**
	 * Execute the operation.
	 * 
	 * @param op
	 * @return
	 */
	protected void execute(Operation op) {
		switch (op.getType()) {
		case ADD:
			AddOperation add = (AddOperation) op;
			log("Executing add recipe operation: " + add.getRecipe(), WARN);
			addRecipe(add.getRecipe());
			break;
		case REMOVE:
			RemoveOperation del = (RemoveOperation) op;
			log("Executing delete recipe operation: " + del.getRecipeTitle(), WARN);
			removeRecipe(del.getRecipeTitle());
			break;
		}
	}

	/**
	 * leader code for serving a request to a user.
	 * 
	 * Invoked by a client to apply an operation to the recipes' service
	 * Operation may be: AddOpperation (to add a recipe) or RemoveOperation (to
	 * remove a recipe)
	 * 
	 * @param Operation
	 *            : operation to apply to recipes
	 * @return RequestResponse: the leaderId and a boolean that indicates if
	 *         operation succeeded or not
	 * @throws RemoteException
	 */
	private RequestResponse leaderRequest(Operation operation) {
		log("LeaderRequest step 1: Store to my own log.", DEBUG);
		log("Appending operation into entry log.", WARN);
		persistentState.addEntry(operation);

		final int indexCommitRequired = persistentState.getLastLogIndex();
		log("The last index is the one we will need to get: " + indexCommitRequired, DEBUG);

		log("LeaderRequest Step 2 : Start sending append entries to the rest of the cluster.\n\tThere is already "
				+ "A thread doing this, so we just have to wait for the thread to get to the current operation and break.",
				DEBUG);

		// Awake heartbeaters to start sending everything
		for ( RaftThread t : heartbeaters ) {
			t.awakeThread();
		}

		do {
			if ( state != RaftState.LEADER ) {
				// If we have changed of state unsuccessful request.
				return new RequestResponse (leader, false);
			}

			// Commit index MUST be updated when persisted at threads.
			if ( indexCommitRequired <= commitIndex ) {
				log("The entry " + indexCommitRequired + " has been committed", DEBUG);
				break;
			}

			try {
				Thread.sleep(10); // Wait to avoid excessive cpu usage.
			} catch (Exception e) {
				// nop, if we have been interrupted to stop this thread because
				// of a state change, we will be stopped on the first if.
			}
		} while (true);

		log("Successfully appended to log.", DEBUG);
		// If we have reached this point, the entry has been committed.
		return new RequestResponse(getServerId(), true);
	}

	/**
	 * DELETE : persistent state can do this, just keep it until we are sure we
	 * don't need it.
	 * 
	 * @param index
	 *            the index of the log which we try to know if it is committed.
	 * @return true if it has been committed.
	 * 
	 *         // private boolean isIndexCommitted(int indexCommitRequired) { //
	 *         int requiredCount = 1 + (numServers / 2); // At least one more
	 *         than a half. // int count = 0; // for ( Host h : otherServers ) {
	 *         // int otherIndex = matchIndex.getIndex(h.getId()); // // // If
	 *         the index is higher than the required one, increase count. // if
	 *         ( otherIndex >= indexCommitRequired ) { // count ++; // } // //
	 *         If already at half the cluster, it is committed. // if ( count ==
	 *         requiredCount ) { // return true; // } // } // return false; // }
	 */

	/**
	 * Leader code on invokation of an appendEntries from another leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat
	 * 
	 * @param term
	 *            : leader's term
	 * @param leaderId
	 *            : so follower can redirect clients
	 * @param prevLogIndex
	 *            : index of log entry immediately preceding new ones
	 * @param prevLogTerm
	 *            : term of prevLogIndex entry
	 * @param entries
	 *            : log entries to store (empty for heartbeat: may send more
	 *            than one for efficiency)
	 * @param leaderCommit
	 *            : leader's commitIndex
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update
	 *         itself), success (true if follower contained entry matching
	 *         prevLongIndex and prevLogTerm)
	 * @throws RemoteException
	 */
	private AppendEntriesResponse leaderAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {
		if (persistentState.getCurrentTerm() == term) {
			// This is impossible, and means that there is some error on the
			// code.
			// This could only happen if:
			// 1) The leader sends a message to himself.
			// 2) There are two leaders for a given term.
			log("TWO LEADERS AT THE TERM " + term, ERROR);
			return null;
		}

		else if (persistentState.getCurrentTerm() < term) {
			// Should become follower if ownterm < term.
			synchronized (guard) {
				changeState(RaftState.FOLLOWER);
				// Follower append entries already sets the term and leader id.
			}

			// And return follower response to the new leader.
			return followerAppendEntries(term, leaderId, prevLogIndex,
					prevLogTerm, entries, leaderCommit);
		}

		else {
			// There is a node at the cluster believing he is the leader, but
			// actually is not.
			log(" >>>>>>>>>>>>>>>>>> THE OTHER SERVER HAS A LOWER TERM!", ERROR);
			return new AppendEntriesResponse(Math.max(term,
					persistentState.getCurrentTerm()), false);
		}
	}

	/**
	 * Sets the leader.
	 * 
	 * @param leaderId
	 */
	private void setLeader(String leaderId) {
		if (leaderId != null) {
			// Check for a new leader.
			if (!leaderId.equals(leader)) {
				// If change of leader
				log("Accepted leader " + leaderId, INFO);
				leader = leaderId;

				// Set to follower if a different leader from us.
				if ( !getServerId().equals(leaderId) ) {
					changeState(RaftState.FOLLOWER);
				}

			}
		} else {
			if (leader != null) {
				// If we remove the leader.
				log("Leader rejected " + leader, INFO);
				leader = null;
			}
		}

	}

	/**
	 * Candidate code on invokation of an appendEntries from a leader.
	 * 
	 * Invoked by leader to replicate log entries; also used as heartbeat
	 * 
	 * @param term
	 *            : leader's term
	 * @param leaderId
	 *            : so follower can redirect clients
	 * @param prevLogIndex
	 *            : index of log entry immediately preceding new ones
	 * @param prevLogTerm
	 *            : term of prevLogIndex entry
	 * @param entries
	 *            : log entries to store (empty for heartbeat: may send more
	 *            than one for efficiency)
	 * @param leaderCommit
	 *            : leader's commitIndex
	 * @return AppendEntriesResponse: term (currentTerm, for leader to update
	 *         itself), success (true if follower contained entry matching
	 *         prevLongIndex and prevLogTerm)
	 * @throws RemoteException
	 */
	private AppendEntriesResponse candidateAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {

		// If equal or bigger term than ours, become follower
		if (term >= persistentState.getCurrentTerm()) {
			// Should become follower if ownterm < term.
			synchronized (guard) {
				log("Accepted leader " + leaderId, WARN);
				changeState(RaftState.FOLLOWER);
			}

			// And return follower response to the new leader.
			return followerAppendEntries(term, leaderId, prevLogIndex,
					prevLogTerm, entries, leaderCommit);
		}
		// If smaller log entry, answer the stale leader accordingly.
		else {
			// There is a node at the cluster believing he is the leader, but
			// actually is not.
			log(">>>> IM A CANDIDATE AND THE OTHER SERVER WITH LOWER TERM BELIEVES HE IS A LEADER",
					ERROR);
			return new AppendEntriesResponse(Math.max(term,
					persistentState.getCurrentTerm()), false);
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
		log("<<<<<<<<<<<<<<< Received heartbeat", INFO);
		leaderWatcher.onHeartbeat();
	}

	//
	// State changes
	//

	/**
	 * Executed when a change of state occurs.
	 * 
	 * @param state
	 */
	private void changeState(RaftState state) {
		// Stop heartbeaters if any.
		for (HeartBeatSenderThread thread : heartbeaters) {
			thread.stopThread();
		}

		heartbeaters.clear();

		this.state = state;
		switch (state) {
		case LEADER:
			log(">>>>>>>>>>>>>>>>>>> I've become the LEADER <<<<<<<<<<<<<<<<<<<<<<", WARN);
			setLeader(getServerId());
			nextIndex = new CustomIndex(otherServers, persistentState.getLastLogIndex() + 1);

			// Start heartbeaters.
			for (Host h : otherServers) {
				HeartBeatSenderThread t = new HeartBeatSenderThread(h, leaderHeartbeatTimeout);
				heartbeaters.add(t);
				// Do not start heartbeaters to force elections to occur.
				t.start();
			}
			break;
		case FOLLOWER:
			log(">>>>>>>>>>>>>>>>>>>>>>>>>>> I've become a FOLLOWER <<<<<<<<<<<<<<<<<<<<<<<<<", INFO);
			// Grant time to the leader to send heartbeats
			leaderWatcher.onHeartbeat();
			break;
		case CANDIDATE:
			log(">>>>>>>>>>>>>>>>>>>>>>>>>>> I've become a CANDIDATE <<<<<<<<<<<<<<<<<<<<<<<<<", INFO);
			// Grant time to the election before restarting it.
			leaderWatcher.onHeartbeat();
			break;
		}
	}

	//
	// API
	//

	/**
	 * The request vote process to respond another server..
	 */
	@Override
	public RequestVoteResponse requestVote(final long term,
			final String candidateId, final int lastLogIndex,
			final long lastLogTerm) throws RemoteException {
		long currentTerm = persistentState.getCurrentTerm();
		// Return the maximum between our term and the server's one (as we will
		// update ourselves if needed).
		return new RequestVoteResponse((currentTerm < term) ? term
				: currentTerm, grantVote(term, candidateId, lastLogIndex,
						lastLogTerm));
		// Test never leader election.
		// return new RequestVoteResponse ( ( currentTerm < term ) ? term :
		// currentTerm , false);
	}

	/**
	 * Guarded method to obtain a vote.
	 * 
	 * @param term
	 * @param candidateId
	 * @param lastLogIndex
	 * @param lastLogTerm
	 * @return
	 */
	private boolean grantVote(long term, String candidateId, int lastLogIndex, long lastLogTerm) {
		synchronized (guard) {
			//			Receiver implementation:
			//				1. Reply false if term < currentTerm (§5.1)
			if ( term < getCurrentTerm() ) {
				log("Not accepting other leader because his term is less than ours.", WARN);
				return false;
			}

			// If higher term, update our own. This will clean the voted for object.
			if ( term > persistentState.getCurrentTerm() ) {
				persistentState.setCurrentTerm(term);
				changeState(RaftState.FOLLOWER);
				log("Updating my term to " + term, INFO);
			}

			//				2. If votedFor is null or candidateId,
			if ( persistentState.getVotedFor() != null && !persistentState.getVotedFor().equals(candidateId) ) {
				log("Not granting vote because i've already voted to " + persistentState.getVotedFor(), INFO);
				return false;
			}
			//			 and candidate's log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
			if ( fresherLog(lastLogIndex, lastLogTerm) ) {
				log("Not granting vote because our term is fresher: "
						+ "[" + persistentState.getLastLogTerm() + "," + persistentState.getLastLogIndex() + "]" 
						+ " > "
						+ "["	+ lastLogTerm + "," + lastLogIndex + "]", WARN);
				return false;
			}

			// If we are here we can grant him the vote.
			persistentState.setVotedFor(candidateId);

			// Add some time to election timeout.
			leaderWatcher.onHeartbeat();

			log("Granting my vote to " + candidateId, INFO);
			return true;
		}
	}

	/**
	 * @param lastLogIndex
	 * @param lastLogTerm
	 * @return true if my log is fresher than the given index and term values.
	 */
	private boolean fresherLog(int lastLogIndex, long lastLogTerm) {
		if ( persistentState.getLastLogTerm() > lastLogTerm) {
			return true;
		}

		if ( persistentState.getLastLogTerm() < lastLogTerm) {
			return false; 
		}

		return persistentState.getLastLogIndex() > lastLogIndex;
	}

	/**
	 * The server has just received a vote and checks whether he can become the
	 * leader.
	 */
	protected void onReceivedVote() {
		if (receivedVotes.size() >= clusterMajority) {
			// The change state will start the heartbeats.
			log("I've  received " + Integer.toString(receivedVotes.size())
					+ " votes, and the required set size is:"
					+ Integer.toString(clusterMajority), INFO);
			synchronized (guard) {
				changeState(RaftState.LEADER);
			}
		} else {
			log("CANDIDATE WAS NOT VOTED BY A MAJORITY OF SERVERS OF THE CLUSTER. THIS SERVER WILL REMAIN AS A CANDIDATE",
					DEBUG);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see recipesService.raft.Raft#appendEntries(long, java.lang.String, int,
	 * long, java.util.List, int)
	 */
	@Override
	public AppendEntriesResponse appendEntries(long term, String leaderId,
			int prevLogIndex, long prevLogTerm, List<LogEntry> entries,
			int leaderCommit) throws RemoteException {
		log(" Received appendEntry for state: " + state, DEBUG);
		log ("With prev index: [" + prevLogTerm + "," + prevLogIndex + "]" , DEBUG);
		log ("With entries : " + entries , DEBUG);
		switch (state) {
		case FOLLOWER:
			return followerAppendEntries(term, leaderId, prevLogIndex,
					prevLogTerm, entries, leaderCommit);
		case CANDIDATE:
			return candidateAppendEntries(term, leaderId, prevLogIndex,
					prevLogTerm, entries, leaderCommit);
		case LEADER:
			return leaderAppendEntries(term, leaderId, prevLogIndex,
					prevLogTerm, entries, leaderCommit);
		}

		// Should never happen.
		return null;
	}

	@Override
	public RequestResponse Request(Operation operation) throws RemoteException {
		// If follower and candidate, redirect him to the last leader known, so
		// it will retry.
		log("Received request from client", INFO);
		switch (state) {
		default:
		case FOLLOWER:
		case CANDIDATE:
			return new RequestResponse(leader, false);
		case LEADER:
			log("Leader performs the request", INFO);
			return leaderRequest(operation);
		}
	}

	//
	// Other methods
	//

	/**
	 * @return the current term.
	 */
	public long getCurrentTerm() {
		return persistentState.getCurrentTerm();
	}

	/**
	 * @return the leader Id always avoiding null.
	 */
	public String getLeaderId() {
		if (leader == null) {
			return "";
		}
		return leader;/* id of the leader in current term; ������ if no leader */
	}

	/**
	 * @return the localhost id.
	 */
	public String getServerId() {
		return localHost.getId();
	}

	/**
	 * @return the log.
	 */
	public synchronized List<LogEntry> getLog() {
		return persistentState.getLog();
	}

	/**
	 * Logs a message using Sysout, adding info.
	 * 
	 * @param msg
	 */
	public void log(String msg, int severity) {
		if (severity >= DEBUG_LEVEL) {
			System.out.println(getServerId() + " @ "
					+ SIMPLE_DATE_FORMAT.format(new Date()) + " @term "
					+ persistentState.getCurrentTerm() + " : " + msg);
		}
	}

	/**
	 * Based on nextIndex, calculate the commit index.
	 */
	public void recalculateCommitIndex() {
		synchronized (guard) {
			// If not leader wait for leader commit index.
			if ( !isLeader() ) {
				return;
			}

			int lastIndex = commitIndex;
			// For each value that can be commited, check if it is so.
			int lastLogIndex = persistentState.getLastLogIndex();

			// Iterate and search commit index.
			for ( int i = lastIndex ; i <= lastLogIndex ; i++ ) {
				if ( matchIndex.majorityHigherOrEqual(i) ) {
					commitIndex = i;
				}
			}
			// Only if modifications awake thread.
			if ( lastIndex != commitIndex ) {
				log ( "Committed data up to entry " + commitIndex + " , awaking thread.",WARN);
				committer.awakeThread();
			}
		}
	}


	/******************************************************************************
	 * 
	 * V2 Threads
	 * 
	 */

	/**
	 * Auxiliar thread that handles a loop inside its run method.
	 * 
	 * @author josep
	 * 
	 */
	public abstract class RaftThread extends Thread {

		private AtomicBoolean stop = new AtomicBoolean(true);
		private AtomicBoolean sleeping = new AtomicBoolean(false);

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public final void run() {
			// on entering run, set stop to false.
			stop.set(false);
			// If stop is true, finish the thread.
			while (!stop.get()) {
				if ( connected.get() ) {
					try {
						// Execute abstract operation that performs its work.
						onIteration();
					} catch (Exception e){
						// Nop
						e.printStackTrace();
					}
				}

				// Do not sleep if the thread has been stopped during
				// onIteration.
				if (stop.get()) {
					break;
				}

				// Sleep until needed again.
				doSleep();
			}
			onGoodBye();
		}

		/**
		 * Lets subclasses do things on end of the thread.
		 */
		protected void onGoodBye() {
			// Nop
		}

		/**
		 * Sleeps until the thread is needed again.
		 */
		private final void doSleep() {
			try {
				sleeping.set(true);
				long l = getAwakeTimestamp() - System.currentTimeMillis();
				// Minimum sleep for 10 millis.
				l = Math.max(l, 10l);
				sleep(l);
			} catch (InterruptedException e) {
				// Thread has been awoken by another one to start working ASAP.
			} finally {
				sleeping.set(false);
			}
		}

		/**
		 * Stops the thread.
		 */
		public final void stopThread() {
			stop.set(true);
			// To make it finish as soon as possible.
			awakeThread();
		}

		/**
		 * Awakes the thread if it is sleeping to start working ASAP.
		 */
		public final void awakeThread() {
			// Interrupt if thread is sleeping needed.
			if (sleeping.get()) {
				interrupt();
			}
		}

		/**
		 * Executed every time the thread has to be awakened.
		 */
		protected abstract void onIteration();

		/**
		 * @return the timestamp at which the thread should be awoken.
		 */
		protected abstract long getAwakeTimestamp();
	}

	/**
	 * Thread managing heartbeat sending to each host.
	 */
	public class HeartBeatSenderThread extends RaftThread {

		/** The host to whom we send him the heartbeats. */
		private Host host;

		/** The interval that should be delayed between heartbeats. */
		private long interval;

		/**
		 * Default constructor takes a host and an interval to start sending
		 * heartbeats.
		 * 
		 * @param host
		 * @param interval
		 */
		public HeartBeatSenderThread(Host host, long interval) {
			this.interval = interval;
			this.host = host;
		}

		/**
		 * Sends a heartbeat to the given host.
		 */
		@Override
		protected void onIteration() {
			synchronized (guard) {
				// the runnable can be ran if the term hasn't changed and
				// localhost is leader
				if ( !isLeader() ) {
					log("Stopping heartbeat thread because we are not the current leader.", WARN);
					stopThread();
					return;
				}
			}

			int lastLogIndex,prevLogindex;
			long prevLogTerm,term;
			List<LogEntry> entries;

			// Load in a guarded section
			int nextIndexInt = nextIndex.getIndex(host.getId());

			synchronized (guard) {
				// My term
				term = getCurrentTerm();

				// Get the last index in our log.
				lastLogIndex = persistentState.getLastLogIndex();

				// * If last log index ≥ nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				if ( lastLogIndex >= nextIndexInt ) {
					entries = persistentState.getLogEntries(nextIndexInt); // Starts at position 1.
				}
				// No entries to send.
				else {
					entries = new ArrayList<LogEntry>();
				}

				// Get prev index and term for the append.
				prevLogindex = nextIndexInt - 1;
				prevLogTerm = persistentState.getTerm(prevLogindex);
			}

			try {
				log("Starting to send heartbeat", DEBUG);
				AppendEntriesResponse appendResponse = communication.appendEntries(host,
						term, leader, prevLogindex,
						prevLogTerm, entries, commitIndex); // Send entries from nextindex on.

				log("Sent heartbeat to " + host.getId(), DEBUG);

				if ( entries.size() > 0 && appendResponse.isSucceeded() ) {
					log ("Sent to " + host.getId() + " entries from index " + nextIndexInt + " containing : " + entries, WARN);
				}

				// Controlling null pointer exception
				if (appendResponse == null) {
					// Do not retry, the other host has returned null, but the
					// communication hasn't failed. This should never happen.
					log("<<<<<<<<< RECEIVED NULL APPEND RESPONSE FROM HOST " + host + ". THIS SHOULD NEVER HAPPEN.", ERROR);
					return;
				}

				// If the other server has a higher term, we are out of date.
				if (persistentState.getCurrentTerm() < appendResponse.getTerm()) {
					log("Host " + host + " had a higher term: " + appendResponse.getTerm(), WARN);
					// I've no leader
					synchronized (guard) {
						changeState(RaftState.FOLLOWER);
						setLeader(null);
						persistentState.setCurrentTerm(appendResponse.getTerm());
					}
					return;
				}


				//				* If successful: update nextIndex and matchIndex for follower (§5.3)
				if ( appendResponse.isSucceeded() ) {
					log("<<<<<<<<<<< AppendEntries accepted from host " + host,	INFO);
					synchronized (guard) {
						// Update indexes
						if ( matchIndex.getIndex(host.getId()) < lastLogIndex ) {
							log("Updating match index to " + lastLogIndex,WARN);
							matchIndex.setIndex(host.getId(), lastLogIndex);
							// Only after recalculate commit index is this one.
							recalculateCommitIndex();
						}
						if ( nextIndexInt < lastLogIndex + 1) {
							log("Updating next index to " + (lastLogIndex + 1),WARN);
							nextIndex.setIndex(host.getId(), lastLogIndex + 1);
						}
					}
				}
				//				* If AppendEntries fails because of log inconsistency:
				//				decrement nextIndex and retry (§5.3)
				else {
					log("<<<<<<<<<<< AppendEntries rejected from host " + host + " decreasing its index to " + (nextIndexInt - 1), ERROR);
					synchronized (guard) {
						nextIndex.decrease(host.getId());						
					}
				}


			} catch (Exception e) {
				// Retry if some error has happened (probably IOException).
				log ("Exception happened during the heartbeat delivery: " + e.getClass(), INFO);
				//				e.printStackTrace();
				return;
			}
		}

		@Override
		protected long getAwakeTimestamp() {
			// Now + interval.
			return System.currentTimeMillis() + interval;
		}
	}

	public class VoteRequesterThread extends RaftThread {

		/** The host to whom we send him the heartbeats. */
		private Host host;

		/** The term of the election. */
		private long term;

		/**
		 * Default constructor takes a host and an interval to start sending
		 * heartbeats.
		 * 
		 * @param host
		 * @param interval
		 */
		public VoteRequesterThread(Host host, long term) {
			this.host = host;
			this.term = term;
		}

		@Override
		protected void onIteration() {
			// If the term has been updated to a newer one, stop the execution.
			synchronized (guard) {
				if ( isLeader() ) {
					log("Stopping vote requester because we are already the leader." , INFO);
					stopThread();
					return;
				}
				if (term < persistentState.getCurrentTerm() ) {
					log("Stopping vote requester because the term has been incremented." , INFO);
					stopThread();
					return;
				}
			}

			try {
				RequestVoteResponse voteResponse = communication.requestVote(host, term,
						getServerId(), persistentState.getLastLogIndex(),
						persistentState.getLastLogTerm());;
						synchronized (guard) {
							log("Requested vote of server " + host.getId(), INFO);

							// Controlling null pointer exception
							if (voteResponse == null) {
								log("NULL VOTE RESPONSE FROM " + host, ERROR);
								// Do retry.
								return;
							}

							// If the vote is granted
							if (voteResponse.isVoteGranted()) {
								// Add to receivedVotes.
								receivedVotes.add(host);

								// Notify the RaftConsensus algorithm that the server
								// has received a vote.
								onReceivedVote();
							}

							// The execution has ended successfully (even if the vote is not
							// granted).
							// Stop the thread.
							stopThread();
						}
			} catch (Exception e) {
				// If an exception is to occur (no response from server or
				// communication exception) retry.
				return;
			}
		}

		@Override
		protected long getAwakeTimestamp() {
			// Wait 10 millis before retrying to get the vote if it has been
			// unsuccessful.
			return 10;
		}
	}

	public class LeaderWatcherThread extends RaftThread {

		/** The timeout timestamp */
		private AtomicLong timeout = new AtomicLong();

		/**
		 * The interval between a heartbeat and the assumption that the leader
		 * is disconnected.
		 */
		private long interval;

		/**
		 * 
		 * @param term
		 *            to check for newer terms.
		 * @param interval
		 */
		public LeaderWatcherThread(long interval) {
			this.interval = interval;
			long delay = interval + (rnd.nextLong() % interval);
			// Add randomization and interval to now.
			timeout.set(System.currentTimeMillis()  + delay);
		}

		/**
		 * Called when the leader sends a heartbeat. Will update the timeout to
		 * the given moment.
		 */
		public void onHeartbeat() {
			timeout.set(interval + System.currentTimeMillis());
		}

		@Override
		protected void onIteration() {
			// Set timeout for next iteration.
			long timeoutValue = timeout.getAndSet(interval + System.currentTimeMillis());

			// If not connected ignore.
			if ( !connected.get() ) {
				log("Not starting an election because we are not currently connected", VERBOSE);
				return;
			}

			// If leader, don't start an election
			if ( isLeader() ) {
				log("Not starting an election because we are the current leader", VERBOSE);
				return;
			}

			// Else, do it if possible.
			if ( timeoutValue <= System.currentTimeMillis() ) {
				log("Starting an election from thread", INFO);
				startElection();
			} else {
				log("Not starting an election because " + timeoutValue + " is greater than the current timestamp : " + System.currentTimeMillis(), VERBOSE);
			}
		}

		@Override
		protected long getAwakeTimestamp() {
			return timeout.get();
		}

		@Override
		protected void onGoodBye() {
			log("################# FINISHING Leader Watcher Thread!!!",ERROR);
		}
	}

	/**
	 * Commits data into the persistent state. Will only be stopped at
	 * disconnect function.
	 */
	public class CommitterThread extends RaftThread {
		@Override
		protected void onIteration() {
			synchronized ( guard ) {
				log("Getting entries from point " + (lastApplied + 1), VERBOSE);
				List<LogEntry> entries = persistentState.getLogEntries(lastApplied + 1);
				for ( LogEntry entry : entries ) {
					// while last applied is less than commit index, commit data.
					if ( lastApplied - 1 < commitIndex ) {
						log("Committing index " + lastApplied, DEBUG);
						Operation op = entry.getCommand();
						RaftConsensus.this.execute(op);
						lastApplied++;
					} else {
						log("Won't commit any more entries because commitIndex has already been reached: " + lastApplied + " == " + commitIndex, DEBUG);
						break;
					}
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see recipesService.raft.RaftConsensus.RaftThread#getAwakeTimestamp()
		 */
		@Override
		protected long getAwakeTimestamp() {
			// Awoken every 5 seconds. Even though, the other threads can awaken
			// him by issuing an awakeThread.
			return 5000l;
		}
	}

	/**
	 * Custom index to calculate correctly if a majority exists during cluster operation
	 */
	public class CustomIndex extends Index {

		public CustomIndex(List<Host> servers, int intialIndex) {
			super(servers, intialIndex);
		}

		public boolean majorityHigherOrEqual(int n){
			int count = 1; // The leader himself
			for (Host h : otherServers){
				int i = getIndex(h.getId());
				if (Integer.valueOf(i) >= n){
					count++;
					// For optimization.
					if ( count >= clusterMajority) {
						return true;
					}
				}
			}
			return false;
		}

	}
}