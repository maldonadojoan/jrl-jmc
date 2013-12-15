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

	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(
			"HH:MM:ss.SSS");

	private static final Object guard = new Object();

	private static final int VERBOSE = 0;
	private static final int DEBUG = 1;
	private static final int INFO = 2;
	private static final int WARN = 3;
	private static final int ERROR = 4;

	private static final int DEBUG_LEVEL = WARN; // Everything from warn to
													// above will be logged.

	// current server
	private Host localHost;

	//
	// STATE
	//

	// raft persistent state state on all servers
	protected PersistentState persistentState;

	// raft volatile state on all servers
	private int commitIndex = 0; // index of highest log entry known to be
									// committed (initialized to 0, increases
									// monotonically)
	private int lastApplied = 0; // index of highest log entry applied to state
									// machine (initialized to 0, increases
									// monotonically)

	// Cooking Recipes
	private CookingRecipes recipes = new CookingRecipes();

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
										// server as candidate in a leader
										// election

	//
	// LEADER
	//

	// Volatile state on leaders
	private Index nextIndex; // for each server, index of the next log entry to
								// send to that server (initialized to leader
								// last log index + 1)
	private Index matchIndex; // for each server, index of highest log known to
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

	private int requiredVotes;

	private boolean connected = false;

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

	public RaftConsensus(long electionTimeout) { // electiontimeout is a
													// parameter in
													// config.properties file
		// set electionTimeout
		this.electionTimeout = electionTimeout;

		// set leaderHeartbeatTimeout
		this.leaderHeartbeatTimeout = electionTimeout / 3; // UOCTODO: Cal
															// revisar-ne el
															// valor
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
		requiredVotes = (numServers / 2) + 1;
	}

	// connect
	public void connect() {
		/*
		 * ACTIONS TO DO EACH TIME THE SERVER CONNECTS (i.e. when it starts or
		 * after a failure or disconnection)
		 */
		// Server starts always as FOLLOWER
		log("Connected", INFO);
		
		if ( matchIndex == null ) {
			matchIndex = new Index(otherServers, 0);
		}
		
		synchronized (guard) {
			setLeader(null);
			changeState(RaftState.FOLLOWER);

			connected = true;

			// Start committer and leader watcher threads.
			committer.start();
			leaderWatcher = new LeaderWatcherThread(electionTimeout);
			leaderWatcher.start();
		}
	}

	/**
	 * Called on a disconnection of the server.
	 */
	public void disconnect() {
		log("<<<<<<<<<<<<< Disconnected at term "
				+ persistentState.getCurrentTerm() + " with leader " + leader
				+ " >>>>>>>>>>>", ERROR);

		// Stop vote requesters.
		for (RaftThread t : voteRequesters) {
			t.stopThread();
		}

		// Stop heartbeaters.
		for (RaftThread t : heartbeaters) {
			t.stopThread();
		}

		// Stop the committer.
		committer.stopThread();

		// Stop the leader watcher.
		leaderWatcher.stopThread();

		// Stop all threads.
		synchronized (guard) {
			connected = false;
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
		final long term;
		log("Start election guarded interval.", DEBUG);
		synchronized (guard) {
			// 2-Change to candidate state
			if (state == RaftState.CANDIDATE) {
				log(">>>>>>>> The election ended without choosing a leader, therefore, it will be restarted <<<<<<<<<< ",
						WARN);
			} else {
				changeState(RaftState.CANDIDATE);
				setLeader(null);
			}
			log("STARTING ELECTION ON HOST " + getServerId(), WARN);

			// 1-Increment current term
			persistentState.nextTerm();
			term = persistentState.getCurrentTerm();
			log("Incrementing my term to " + term, WARN);

			// Clear list of received votes
			this.receivedVotes = new HashSet<Host>();

			// 3-Vote for self
			this.persistentState.setVotedFor(getServerId());
			this.receivedVotes.add(localHost);

			// Stop the previous requester thread if needed.
			for (VoteRequesterThread thread : voteRequesters) {
				thread.stopThread();
			}

			// Clear the array
			voteRequesters.clear();

			// Refill it for current term
			for (Host host : otherServers) {
				VoteRequesterThread thread = new VoteRequesterThread(host, term);
				voteRequesters.add(thread);
				thread.start();
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
	private AppendEntriesResponse followerAppendEntries(long term,
			String leaderId, int prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, int leaderCommit) {

		// If null log prior to this, set all to 0.
		if (prevLogIndex == -1) {
			prevLogIndex = 0;
		}

		// Same term, with different leader update it.
		if (term == persistentState.getCurrentTerm()) {
			log("Received append entries for my term. Updating leader if needed",
					DEBUG);
			synchronized (guard) {
				setLeader(leaderId);
			}
		}

		// If stale leader, notify him
		if (term < persistentState.getCurrentTerm()) {
			log("Received append entry with older term.", ERROR);
			return new AppendEntriesResponse(persistentState.getCurrentTerm(),
					false);
		} else if (term > persistentState.getCurrentTerm()) {
			log("Received append entry with newer term.", ERROR);
			synchronized (guard) {
				persistentState.setCurrentTerm(term);
				setLeader(leaderId);
			}
		}

		// Update commit index.
		commitIndex = leaderCommit;
		
		// Start committer thread.
		committer.awakeThread();

		// If not a stale leader, count from now the heartbeat.
		onHeartbeat();

		// If it fits our need in the log entries, store it.
		if (prevLogIndex == persistentState.getLastLogIndex()
				&& prevLogTerm == persistentState.getLastLogTerm()) {
			// Store it to log.
			if (entries == null) {
				log(">>>>>>>>>>>>>>>>>>>>>>>>> Null entries received!", ERROR);
				return new AppendEntriesResponse(term, true);
			}

			// If entries available
			if (entries.size() > 0) {
				// Start persisting them in our log.
				for (LogEntry entry : entries) {
					log("Appending new entry to follower persistentState : "
							+ entry + " @ " + persistentState.getLastLogIndex(),
							WARN);
					persistentState.appendEntry(entry);
				}
				// Start committer thread.
				committer.awakeThread();
			} else {
				log("Did not receive any entry", INFO);
			}
			return new AppendEntriesResponse(term, true);
		}
		// If not correct, notify the leader that we need a previous log first.
		else {
			log("prevLogIndex == persistentState.getLastLogIndex() && prevLogTerm == persistentState.getLastLogTerm() is false because:",
					ERROR);
			log("prevLogIndex = " + prevLogIndex, ERROR);
			log("persistentState.getLastLogIndex() = "
					+ persistentState.getLastLogIndex(), ERROR);
			log("prevLogTerm = " + prevLogTerm, ERROR);
			log("persistentState.getLastLogTerm() = "
					+ persistentState.getLastLogTerm(), ERROR);
			return new AppendEntriesResponse(term, false);
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
			recipes.addRecipe(add.getRecipe());
			break;
		case REMOVE:
			RemoveOperation del = (RemoveOperation) op;
			log("Executing delete recipe operation: " + del.getRecipeTitle(),
					WARN);
			recipes.removeRecipe(del.getRecipeTitle());
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
		log("The last index is the one we will need to get: "
				+ indexCommitRequired, DEBUG);

		log("LeaderRequest Step 2 : Start sending append entries to the rest of the cluster.\n\tThere is already "
				+ "A thread doing this, so we just have to wait for the thread to get to the current operation and break.",
				DEBUG);

		do {
			if (state != RaftState.LEADER) {
				// If we have changed of state unsuccessful request.
				return new RequestResponse(leader, false);
			}

			// Commit index MUST be updated when persisted at threads.
			if (indexCommitRequired >= commitIndex) {
				log("The entry " + indexCommitRequired + " has been committed",
						DEBUG);
				break;
			}

			// Alternative way
			// if ( isIndexCommitted(indexCommitRequired) ) {
			// break;
			// }
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
			if (!leaderId.equals(leader)) {
				// If change of leader
				log("Accepted leader " + leaderId, ERROR);
				leader = leaderId;
			}
		} else {
			if (leader != null) {
				// If we remove the leader.
				log("Leader rejected " + leader, ERROR);
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
		// TODO Get HeartbeatTimeoutThread and notify him.
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
			log(">>>>>>>>>>>>>>>>>>> I've become the LEADER <<<<<<<<<<<<<<<<<<<<<<",
					WARN);
			setLeader(getServerId());
			nextIndex = new Index(otherServers, persistentState.getLastLogIndex() + 1);

			// Start heartbeaters.
			for (Host h : otherServers) {
				HeartBeatSenderThread t = new HeartBeatSenderThread(h,
						leaderHeartbeatTimeout,
						persistentState.getCurrentTerm());
				heartbeaters.add(t);
				t.start();
			}
			break;
		case FOLLOWER:
			log("I've become a FOLLOWER", WARN);
			// What to do?
			break;
		case CANDIDATE:
			log("I've become a CANDIDATE", WARN);

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
	private boolean grantVote(long term, String candidateId, int lastLogIndex,
			long lastLogTerm) {
		synchronized (guard) {
			// If higher term, update our own. This will clean the voted for
			// object.
			if (term > persistentState.getCurrentTerm()) {
				persistentState.setCurrentTerm(term);
				log("Updating my term to " + term, WARN);
			}

			// If we have already voted in this term, don't grant the vote.
			if (persistentState.getVotedFor() != null) {
				log("Not granting vote because i've already voted to "
						+ persistentState.getVotedFor(), WARN);
				return false;
			}

			// If we have a higher term log entry, don't give him our vote.
			if (lastLogTerm < persistentState.getLastLogTerm()) {
				log("Not granting vote because i'm at a newer term "
						+ persistentState.getLastLogTerm() + " > "
						+ lastLogTerm, WARN);
				return false;
			}

			// If we are in the same term but we have a newer log index, don't
			// give the vote.
			if (lastLogTerm == persistentState.getLastLogTerm()
					&& lastLogIndex < persistentState.getLastLogIndex()) {
				log("Not granting vote because i'm at a newer term "
						+ persistentState.getLastLogIndex() + " > "
						+ lastLogIndex, WARN);
				return false;
			}

			// If we are here we can grant him the vote.
			persistentState.setVotedFor(candidateId);
			// TODO add some time to election timeout.

			log("Granting my vote to " + candidateId, WARN);
			return true;
		}
	}

	/**
	 * The server has just received a vote and checks whether he can become the
	 * leader.
	 */
	protected void onReceivedVote() {
		if (receivedVotes.size() >= requiredVotes) {
			// The change state will start the heartbeats.
			log("I've  received " + Integer.toString(receivedVotes.size())
					+ " votes, and the required set size is:"
					+ Integer.toString(requiredVotes), ERROR);
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
		case CANDIDATE: // TODO WHERE SHOULD CANDIDATE REDIRECT?
			return new RequestResponse(leader, false);
		case LEADER:
			log("Leader performs the request", INFO);
			return leaderRequest(operation);
		}
	}

	//
	// Other methods
	//
	public String getServerId() {
		return localHost.getId();
	}

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
			System.err.println(getServerId() + " @ "
					+ SIMPLE_DATE_FORMAT.format(new Date()) + " @term "
					+ persistentState.getCurrentTerm() + " : " + msg);
		}
	}

	//
	// Other methods
	//
	public long getCurrentTerm() {
		return persistentState.getCurrentTerm();
	}

	public String getLeaderId() {
		if (leader == null) {
			return "";
		}
		return leader;/* id of the leader in current term; “” if no leader */
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
				if (connected) {
					// Execute abstract operation that performs its work.
					onIteration();
				}

				// Do not sleep if the thread has been stopped during
				// onIteration.
				if (stop.get()) {
					break;
				}

				// Sleep until needed again.
				doSleep();
			}
		}

		/**
		 * Sleeps until the thread is needed again.
		 */
		private final void doSleep() {
			try {
				sleeping.set(true);
				sleep(getAwakeTimestamp() - System.currentTimeMillis());
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
	 * A thread that runs periodically.
	 */
	public abstract class PeriodicRaftThread extends RaftThread {

		/** 5 milliseconds default retry timeout. */
		private static final long DEFAULT_RETRY_TIMEOUT = 5l;

		/** The interval between executions. */
		private long interval;

		/** The next timestamp at which the thread should be awoken */
		private long nextWake;

		public PeriodicRaftThread(long interval) {
			this.interval = interval;
		}

		@Override
		protected final void onIteration() {
			nextWake = calculateNextWakeTime();
			if (!onPeriodicIteration()) {
				nextWake = System.currentTimeMillis() + DEFAULT_RETRY_TIMEOUT;
			}
		}

		@Override
		protected long getAwakeTimestamp() {
			if (nextWake < System.currentTimeMillis()) {
				// if this happens, we will skip a beat...
				nextWake = calculateNextWakeTime();

				// Alternatively we could do:
				nextWake = System.currentTimeMillis();
			}
			return nextWake;
		}

		/**
		 * This method is called before the operation so the delay in the
		 * operation won't
		 * 
		 * @return
		 */
		protected long calculateNextWakeTime() {
			return System.currentTimeMillis() + interval;
		}

		/**
		 * @return false if there has been an error, true if not (even if not
		 *         successful).
		 */
		protected abstract boolean onPeriodicIteration();
	}

	/**
	 * Thread managing heartbeat sending to each host.
	 */
	public class HeartBeatSenderThread extends PeriodicRaftThread {

		/** The host to whom we send him the heartbeats. */
		private Host host;

		/** The term in which the thread was created and we were the leaders. */
		private long electionTerm;

		/**
		 * Default constructor takes a host and an interval to start sending
		 * heartbeats.
		 * 
		 * @param host
		 * @param interval
		 */
		public HeartBeatSenderThread(Host host, long interval, long term) {
			super(interval);
			this.host = host;
			this.electionTerm = term;
		}

		/**
		 * Sends a heartbeat to the given host.
		 */
		@Override
		protected boolean onPeriodicIteration() {
			synchronized (guard) {
				// the runnable can be ran if the term hasn't changed and
				// localhost is leader
				if (electionTerm == persistentState.getCurrentTerm()
						&& isLeader()) {
					stopThread();
					return true;
				}

				log("Doing the sending heartbeat " + host.getId() + "@"
						+ host.getAddress(), DEBUG);
				int lastIndex = persistentState.getLastLogIndex();

				// Get index and term.
				int index = nextIndex.getIndex(host.getId());
				long term = persistentState.getTerm(index);

				// Load entries or null list if it wasn't possible
				List<LogEntry> entries = null;
				try {
					entries = persistentState
							.getLogEntries((index >= 0) ? index + 1 : 1); // Starts
																			// at
																			// position
																			// 1.
				} catch (Exception e) {
					e.printStackTrace();
					entries = new ArrayList<LogEntry>();
				}

				AppendEntriesResponse appendResponse;
				try {
					appendResponse = communication.appendEntries(host,
							persistentState.getCurrentTerm(), leader, index,
							term, entries, commitIndex); // Send entries from
															// nextindex

				} catch (Exception e) {
					// Retry if some error has happened (probably IOException).
					return false;
				}

				// Controlling null pointer exception
				if (appendResponse == null) {
					// Do not retry, the other host has returned null, but the
					// communication hasn't failed. This should never happen.
					log("<<<<<<<<< RECEIVED NULL APPEND RESPONSE FROM HOST "
							+ host + ". THIS SHOULD NEVER HAPPEN.", ERROR);
					return true;
				}

				// If the other server has a higher term, we are out of date.
				if (persistentState.getCurrentTerm() < appendResponse.getTerm()) {
					log("Host " + host + " had a higher term: "
							+ appendResponse.getTerm(), WARN);
					// I've no leader
					synchronized (guard) {
						setLeader(null);
						persistentState
								.setCurrentTerm(appendResponse.getTerm());
						changeState(RaftState.FOLLOWER);
					}
				}

				// If AppendEntries succeeded
				if (appendResponse.isSucceeded()) {
					log("<<<<<<<<<<< AppendEntries accepted from host " + host,
							INFO);
					// Always update the matchindex
					matchIndex.setIndex(host.getId(), lastIndex);
					nextIndex.setIndex(host.getId(), lastIndex);
				}
				// If failed to persist, the other one needs a higher
				else {
					log("<<<<<<<<<<< AppendEntries rejected from host " + host
							+ " decreasing its index.", ERROR);
					nextIndex.decrease(host.getId());
				}
			}

			return true;
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
				if (term < persistentState.getCurrentTerm() || isLeader()) {
					stopThread();
					return;
				}
			}

			try {
				RequestVoteResponse voteResponse;
				synchronized (guard) {
					log("Requesting vote of server " + host.getId(), INFO);
					voteResponse = communication.requestVote(host, term,
							getServerId(), persistentState.getLastLogIndex(),
							persistentState.getLastLogTerm());

					// Controlling null pointer exception
					if (voteResponse == null) {
						log("NULL VOTE RESPONSE FROM " + host, ERROR);
						// Do not retry.
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
				}
			} catch (Exception e) {
				// If an exception is to occur (no response from server or
				// communication exception), return false to retry the runnable.
				e.printStackTrace();
				return;
			}

			// The execution has ended successfully (even if the vote is not
			// granted).
			// Stop the thread.
			stopThread();
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
			timeout.set(interval + System.currentTimeMillis());
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
			// If leader, don't start an election
			if (isLeader()) {
				return;
			}
			// Else, do it if possible.
			else if (System.currentTimeMillis() > timeout.getAndSet(interval
					+ System.currentTimeMillis())) {
				stopThread();
				startElection();
			}
		}

		@Override
		protected long getAwakeTimestamp() {
			return timeout.get();
		}
	}

	/**
	 * Commits data into the persistent state. Will only be stopped at
	 * disconnect function.
	 */
	public class CommitterThread extends RaftThread {
		@Override
		protected void onIteration() {
			synchronized (recipes) {
				log("Getting entries from point " + lastApplied, VERBOSE);
				List<LogEntry> entries = persistentState
						.getLogEntries(lastApplied);
				for (LogEntry entry : entries) {
					// When the last applied equals the commit index. Stop
					// commiting data.
					if (lastApplied == commitIndex) {
						log("Didn't commit any more entries because commitIndex has already been reached",
								INFO);
						break;
					}
					log("Committing index " + lastApplied, WARN);
					Operation op = entry.getCommand();
					RaftConsensus.this.execute(op);
					lastApplied++;
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
}