package virtualSynchrony;

import java.io.Serializable;
import scala.concurrent.duration.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Participant extends AbstractActor {
	
	private final int id;
	public int getId() {
		return id;
	}
	
	private static Object LOCK = new Object();
	private List<ActorRef> group;
	private List<ActorRef> newGroup;			//used for view change flush participants
	private Random rnd = new Random();
	private int sendCount = 0;
	final static int N_MESSAGES = 1;
	final static int VOTE_TIMEOUT = 1000;      // timeout for the votes, ms
	
	// a buffer storing all received chat messages
	private StringBuffer chatHistory = new StringBuffer();
	private List<ChatMsg> buffer =new ArrayList<>();
	  
	
	/* -- Actor constructor  --------------------------------------------------- */
	  
	public Participant(int id) {
		this.id = id;
	}
	static public Props props(int id) {
		return Props.create(Participant.class, () -> new Participant(id));
	}
	
	/* -- Message types ------------------------------------------------------- */
	
	// Start message that informs every chat participant about its peers
	public static class JoinGroupMsg implements Serializable {
		private final List<ActorRef> group;

		public JoinGroupMsg(List<ActorRef> group) {
			this.group = group;
		}
	}
	
	public static class PrintHistoryMsg implements Serializable {}
	
	// A message requesting the peer to start a discussion on his topic
	public static class StartChatMsg implements Serializable {}
	
	public static class ChatMsg implements Serializable {
		    public final int n;          // the number of the reply in the current topic
		    public final int senderId;   // the ID of the message sender
		    public final boolean isStable;
		    public final boolean isFlush;
		    public ChatMsg(int n,int senderId, boolean isStable, boolean isFlush) {
		     
		      this.n = n;
		      this.senderId = senderId;
		      this.isStable = isStable;
		      this.isFlush = isFlush;
		      
		    }
		    
	  }
	
	public static class Timeout implements Serializable {
		public final int senderid;

		public Timeout(int senderid) {
			this.senderid=senderid;
		}}
	
	public static class ViewChange implements Serializable {
		private final List<ActorRef> group;

		public ViewChange(List<ActorRef> group) {
			this.group = group;
		}
	}
	
	public static class ParticipantCrashed implements Serializable {
		public final int crashid;

		public ParticipantCrashed(int crashid) {
			this.crashid = crashid;
		}
	}
	
	private void sendChatMsg(int n)  {
	    sendCount++; //number of messages broadcast
	    
	    ChatMsg m = new ChatMsg(n,this.id, false, false);
	    chatHistory.append(m.senderId+":"+m.n + "m ");
	    
	    ChatMsg m1 = new ChatMsg(n,this.id, true, false);
	    // wait for normal message multicast to complete
	    if(multicast(m)) {
	    	multicast(m1); //multicast stable message
	    }
	}
	
	/* -- Actor behaviour ----------------------------------------------------- */
	
	
	protected boolean crashed = false;          // simulates a crash
	
	private void onJoinGroupMsg(JoinGroupMsg msg) {
		this.group = msg.group;
	}
	private void onStartChatMsg(StartChatMsg msg) {
	    sendChatMsg(0); // start topic with message 0
	  }
	private void onChatMsg(ChatMsg msg)  {
		deliver(msg);  // "deliver" the message to the simulated chat user
	}
	
	private void printHistory(PrintHistoryMsg msg) {
	    System.out.printf("%s: %s\n", this.getSelf().path().name(), chatHistory);
	  }
	
	private void appendToHistory(ChatMsg m) {
		if(!m.isStable) {
			chatHistory.append(m.senderId+":"+m.n + "n ");
		}
		else {
			chatHistory.append(m.senderId+":"+m.n + "s ");
		}
	  }
	
	private boolean multicast(Serializable m) { // our multicast implementation
       List<ActorRef> shuffledGroup = new ArrayList<>(group);
	   Collections.shuffle(shuffledGroup);
	   for (ActorRef p: shuffledGroup) {
		   if (!p.equals(getSelf())) { // not sending to self
			   p.tell(m, getSelf());
			   try {
				   Thread.sleep(rnd.nextInt(10));
			   } 
			   catch (InterruptedException e) {
				   e.printStackTrace();
			   }
		   }
	  }
	   return true;
	}

	private void deliver(ChatMsg m)  {
		// for unstable messages
		if(!m.isStable) {
			this.buffer.add(m);
			appendToHistory(m);
			//set timeout, if timeout occurs it will call crashDetected method 
			setTimeout(VOTE_TIMEOUT,m.senderId);
		}
		//if msg is of flush type
		else if(m.isFlush) { 
			this.newGroup.add(this.group.get(m.senderId));
		}
		// for stable messages
		else if(m.isStable){
			//removing stable msg from buffer
			for (Iterator<ChatMsg> iterator = this.buffer.iterator(); iterator.hasNext(); ) {
				ChatMsg value = iterator.next();
			    if (m.senderId==value.senderId) {
			        iterator.remove();
			    }
			}
//			for (ChatMsg tmp : this.buffer) {
//				if(m.senderId==tmp.senderId) {
//					this.buffer.remove(tmp);
//				}
//			}
			appendToHistory(m);
		}
	    
	}
	
	// schedule a Timeout message in specified time
    private void setTimeout(int time,int senderid) {
      getContext().system().scheduler().scheduleOnce(
          Duration.create(time, TimeUnit.MILLISECONDS),  
          getSelf(),
          new Timeout(senderid), // the message to send
          getContext().system().dispatcher(), getSelf()
          );
    }
    private void onTimeout(Timeout msg) {                           /* Timeout */
	      //if (crashed) return;
    	// TODO sender identified by sender-id is crashed. 
    	// 1) send the group member list to everyone in the group
    	
    	// Tell GM to remove the crashed Participant
    	group.get(0).tell(new ParticipantCrashed(msg.senderid), null);
    			 
	}
	
	private void onViewChange(ViewChange list) {					/* View changed*/
		//TODO 1) stop all multicast
		// 2) multicast all unsatble msgs
		if(!this.buffer.isEmpty()) {
			for(ChatMsg tmp : this.buffer) {
			    chatHistory.append(tmp.senderId+":"+tmp.n + "m ");
			    multicast(tmp);
			}
		}
		// 3) send flush to every one
		ChatMsg m= new ChatMsg(100,this.id,false,true);
		multicast(m);
		// 4) wait for flush from every one in new view
		newGroup= new ArrayList<>();
		try {
			synchronized(LOCK){
				while(!allFlush(list.group)){
					LOCK.wait();	
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// 5) install the view
		this.group=list.group;
	}
	
	//check whether all the active participants have send the flush msg
	private boolean allFlush(List<ActorRef> group) {
		return new HashSet<>(group).equals(new HashSet<>(this.newGroup));
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinGroupMsg.class, this::onJoinGroupMsg)
				.match(PrintHistoryMsg.class, this::printHistory)
				.match(ChatMsg.class,         this::onChatMsg)
				.match(StartChatMsg.class,    this::onStartChatMsg)
				.match(Timeout.class, this::onTimeout)
				.match(ViewChange.class, this::onViewChange)
				.match(ParticipantCrashed.class, this::onParticipantCrashed)
				.build();
	}
	
	/* -- GM behaviour ----------------------------------------------------- */
	private void onParticipantCrashed(ParticipantCrashed msg) {
		if(group.get(msg.crashid) != null ) {
			group.remove(msg.crashid);
			ViewChange update = new ViewChange(group);
			int i=0;
			//TODO change this while loop
			while(i<group.size()) {
				i++;
				group.get(i).tell(update, null);
			}
			
		}
		
		else {
			return;
		}
		
		
	}
	
}
