package virtualSynchrony;

import java.io.Serializable;
import scala.concurrent.duration.Duration;
import virtualSynchrony.newParticipant.JoinGroupMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

public class Participant extends AbstractActor {
	
	private final int id;
	
	private List<ActorRef> group;
	private Random rnd = new Random();
	private int sendCount =0;
	final static int N_MESSAGES = 2;
	final static int VOTE_TIMEOUT = 2000;      // timeout for the votes, ms
	//private int epoch =1;
	private boolean crashAfterReceiveMulticast=false;
	private boolean crashAfterViewChange=false;
	private List<ActorRef> flushGroup = new ArrayList<>();
	
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
		private final int epoch;

		public JoinGroupMsg(List<ActorRef> group, int epoch) {
			this.group = group;
			this.epoch=epoch;
		}
	}
	
	public static class PrintHistoryMsg implements Serializable {}
	
	public static class CreateNewActor implements Serializable {}
	
	public static class JoinNew implements Serializable {
		private final List<ActorRef> group;

		public JoinNew(List<ActorRef> group) {
			this.group = group;
		}
	}
	
	public static class CrashAfterReceiveMulticast implements Serializable {
		private final boolean crashAfterReceiveMulticast; 

		public CrashAfterReceiveMulticast(boolean crashAfterReceiveMulticast) {
			this.crashAfterReceiveMulticast=crashAfterReceiveMulticast;
		}
	}
	
	public static class CrashAfterViewChange implements Serializable {
		private final boolean crashAfterViewChange; 

		public CrashAfterViewChange(boolean crashAfterViewChange) {
			this.crashAfterViewChange=crashAfterViewChange;
		}
	}
	
	// A message requesting the peer to start a discussion on his topic
	public static class StartChatMsg implements Serializable {
		private final boolean sendStable;

		public StartChatMsg(boolean sendStable) {
			this.sendStable=sendStable;
		}
	}
	
	public static class AllowMulticast implements Serializable {
		private final boolean sendStable;

		public AllowMulticast(boolean sendStable) {
			this.sendStable=sendStable;
		}
	}
	
	public static class ChatMsg implements Serializable {
		    public final int n;          // the number of the reply in the current topic
		    public final int senderId;   // the ID of the message sender
		    public final boolean isStable;
		    public final boolean isFlush;
		    public final int view;
		    public ChatMsg(int n,int senderId, boolean isStable, boolean isFlush, int view) {
		     
		      this.n = n;
		      this.senderId = senderId;
		      this.isStable = isStable;
		      this.isFlush = isFlush;
		      this.view = view;
		      
		    }
		    
	  }
	
	public static class Timeout implements Serializable {
		public final int senderid;

		public Timeout(int senderid) {
			this.senderid=senderid;
		}}
	
	public static class FlushTimeout implements Serializable {
		private final List<ActorRef> group;
		private final int view;

		public FlushTimeout(List<ActorRef> group, int view) {
			this.group = group;
			this.view= view;
		}
	}
	
	public static class ViewChange implements Serializable {
		private final List<ActorRef> group;
		private final int view;

		public ViewChange(List<ActorRef> group, int i) {
			this.group = group;
			this.view = i;
		}
	}
	
	public static class ParticipantCrashed implements Serializable {
		public final ActorRef crashActor;

		public ParticipantCrashed(ActorRef crashActor) {
			this.crashActor = crashActor;
		}
	}
	
	
	/* -- Actor behaviour ----------------------------------------------------- */
	
	
	protected boolean crashed = false;          // simulates a crash
	private Cancellable allFlush;
	private Cancellable timerMulticast;
	private int inhibit_sends = 0;
	private int view = 0;
	
	private void onCrashAfterReceiveMulticast(CrashAfterReceiveMulticast msg) {
		this.crashAfterReceiveMulticast = msg.crashAfterReceiveMulticast;
	}
	
	private void onCrashAfterViewChange(CrashAfterViewChange msg) {
		this.crashAfterViewChange = msg.crashAfterViewChange;
	}
	
	private void onJoinGroupMsg(JoinGroupMsg msg) {
		this.group = msg.group;
		if(this.view != msg.epoch) {
			//PrintNewViewMsg(this.group);
			this.view = msg.epoch;
		}
	}

	private void PrintNewViewMsg(List<ActorRef> group2) {
		StringBuffer tmp= new StringBuffer();
		for(ActorRef p: group2) {
			if(!p.path().name().equals("GM0"))
				tmp.append(",");
			tmp.append(p.path().name().replaceAll("\\D+",""));
		}
		System.out.println(this.id+ " install view "+this.view+" "+ tmp.toString()+" "+this.inhibit_sends);
		//System.out.println(" "+this.inhibit_sends);
		
	}
	
	private void onAllowMulticast(AllowMulticast msg) {
		if(this.inhibit_sends==0) {
			this.timerMulticast.cancel();
			getSelf().tell(new StartChatMsg(msg.sendStable),getSelf());
		}
	}
	
	private void onStartChatMsg(StartChatMsg msg) {
		if(sendCount<N_MESSAGES) {
			if(this.inhibit_sends==0) {
			
				sendCount++; //number of messages broadcast
				ChatMsg m = new ChatMsg(sendCount,this.id, false, false, this.view);
				ChatMsg m1 = new ChatMsg(sendCount,this.id, true, false, this.view);
			
				System.out.println(this.id +" send multicast m"+ sendCount + " within " + this.view);
				if(multicast(m,this.group) && msg.sendStable) {
					//multicast stable message
					multicast(m1,this.group);
				}
			}
			else {
				//wait for inhibit_sends to turn to false
				timerMulticast =getContext().system().scheduler().schedule(
						Duration.create(0, TimeUnit.MILLISECONDS),
		    			Duration.create(50, TimeUnit.MILLISECONDS),  
				        getSelf(),
				        new AllowMulticast(msg.sendStable), // the message to send
				        getContext().system().dispatcher(), getSelf()
				        );
			}
		}
	  }
	
	private void onChatMsg(ChatMsg msg)  {
		deliver(msg);  // "deliver" the message to the simulated chat user
	}	
	
	private boolean multicast(Serializable m, List<ActorRef> g) { // our multicast implementation
		
       List<ActorRef> shuffledGroup = new ArrayList<>(g);
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
		if(!m.isStable && !m.isFlush) {
			
			this.buffer.add(m);
			//set timeout, on timeout it will check if stable msg received else it calls crashDetected method 
			setTimeout(VOTE_TIMEOUT,m.senderId);
			
		}
		//if msg is of flush type
		else if(m.isFlush) { 
			//System.out.println(getSelf().path().name()+": flush recived from "+ m.senderId);
			this.flushGroup.add(getSender());
			
			
			
		}
		// for stable messages
		else if(m.isStable && this.view==m.view){
			
			//iterate to unstable messages in buffer 
			for (Iterator<ChatMsg> iterator = this.buffer.iterator(); iterator.hasNext(); ) {
				ChatMsg value = iterator.next();
			    if (m.senderId==value.senderId && m.view==value.view) {
			    	System.out.println(this.id+" deliver multicast m"+m.n+ " from " + m.senderId + " within " + m.view);
			        iterator.remove(); //removing stable msg from buffer
			    }
			}
			if(crashAfterReceiveMulticast) {
				group.get(0).tell(new ParticipantCrashed(getSelf()), null);
			}
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
    	boolean stable= true;
    	
    	//check if participant received stable msg
    	if(!this.buffer.isEmpty()) {
    		for(ChatMsg p: this.buffer) {
    			if(p.senderId==msg.senderid) {
    				stable=false;
    			}
    		}
    	}
    	
    	//if stable msg is not received, tell GM to install new view
    	if(!stable) {
    		// Tell GM to remove the crashed Participant
    		//System.out.println(getSelf().path().name()+": TIMEOUT: stable not recived from " + msg.senderid);
        	group.get(0).tell(new ParticipantCrashed(group.get(msg.senderid)), null);
    	}
    			 
	}

    private void onFlushTimeout(FlushTimeout msg) throws InterruptedException {
    	//PrintNewViewMsg(this.group);
    	if(isAllFlush(msg.group)) {
    		//install new view successfully
    		//this.allFlush.cancel(); 		//stop ticker if flush received from all participants
    		this.view=msg.view;
        	this.inhibit_sends--;
        	this.group=msg.group;
    		PrintNewViewMsg(this.group);
    		//Any message that was sent in view and has not yet been delivered may be discarded
    		this.buffer.clear();
    	}
    	//after time out if there is still someone who did not send the flush
    	else if(this.flushGroup.size() < msg.group.size()) {
    		//PrintNewViewMsg(this.flushGroup);
    		List<ActorRef> tmp= new ArrayList<>();
    		tmp.addAll(msg.group);
    		tmp.removeAll(this.flushGroup);
    		//tell GM that p crashed
    		if(tmp.size()==1) {
    			//System.out.println(tmp.get(0).path().name()+" crashed in " + this.id);
    			group.get(0).tell(new ParticipantCrashed(tmp.get(0)), getSelf());
    		}
    		else {
    			//System.out.println("problem");
    			//PrintNewViewMsg(this.flushGroup);
    			//System.out.println("problem-e");
    		}
    			
    	}
    }
    
	private void onViewChange(ViewChange list) {					/* View changed*/
		if(!crashAfterViewChange) {
		//1) stop multicast
		this.inhibit_sends++;
		
		//this.view=list.view;
		
		if(!this.buffer.isEmpty()) {	//check if buffer is not empty mean there is some unstable msg received by current participant.
			for(ChatMsg tmp : this.buffer) {
			    
			    // 2) multicast unstable msg
			    ChatMsg m= new ChatMsg(tmp.n,tmp.senderId,true,false,tmp.view);
			    multicast(m,this.group); 			
			    
			}
		}
		this.flushGroup.clear();
		//this.flushGroup.addAll(list.group);
		this.flushGroup.add(getSelf());
		
		
		// 3) multicast flush to every one
		ChatMsg m= new ChatMsg(0,this.id,false,true,list.view);
    	if(multicast(m,list.group))		{
    		getContext().system().scheduler().scheduleOnce(
      	          Duration.create(30, TimeUnit.MILLISECONDS),  
      	          getSelf(),
      	          new FlushTimeout(list.group,list.view+1), // the message to send
      	          getContext().system().dispatcher(), getSelf()
      	          );
    	}
    	// adding my self in the flushGroup to check if all members of epoch send the flush
    	//this.flushGroup.add(getSelf());
    	
    	
		// 4) wait for flush from every one in new view
		// 5) install the view
//    	allFlush =getContext().system().scheduler().schedule(
//    			Duration.create(3000, TimeUnit.MILLISECONDS),
//    			Duration.create(5000, TimeUnit.MILLISECONDS),  
//		        getSelf(),
//		        new FlushTimeout(list.group), // the message to send
//		        getContext().system().dispatcher(), getSelf()
//		        );
    	
//    	allFlush =getContext().system().scheduler().scheduleOnce(
//    	          Duration.create(30000, TimeUnit.MILLISECONDS),  
//    	          getSelf(),
//    	          new FlushTimeout(list.group,list.view+1), // the message to send
//    	          getContext().system().dispatcher(), getSelf()
//    	          );
    	
		}
    	
	}
	
	private boolean isAllFlush(List<ActorRef> group) {
		return new HashSet<>(group).equals(new HashSet<>(this.flushGroup));
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinGroupMsg.class, this::onJoinGroupMsg)
				.match(ChatMsg.class,         this::onChatMsg)
				.match(StartChatMsg.class,    this::onStartChatMsg)
				.match(Timeout.class, this::onTimeout)
				.match(ViewChange.class, this::onViewChange)
				.match(ParticipantCrashed.class, this::onParticipantCrashed)
				.match(FlushTimeout.class, this::onFlushTimeout)
				.match(JoinNew.class, this::onJoinNew)
				.match(AllowMulticast.class, this::onAllowMulticast)
				.match(CrashAfterReceiveMulticast.class, this::onCrashAfterReceiveMulticast)
				.match(CrashAfterViewChange.class, this::onCrashAfterViewChange)
				.match(CreateNewActor.class, this::onCreateNewActor)
				.build();
	}
	
	/* -- GM behaviour ----------------------------------------------------- */
	private void onParticipantCrashed(ParticipantCrashed msg) {
		
		//System.out.println(getSelf().path().name()+": "+msg.crashActor.path().name() + ": crashed: installing new view");
		if(group.contains(msg.crashActor) && this.id==0) {
			
			//removing crashed participant from the group
			this.group.remove(msg.crashActor);
			//PrintNewViewMsg(this.group);
			ViewChange update = new ViewChange(group,this.view);
			//System.out.print("/////////////////////////");
			//PrintNewViewMsg(group);
			//tell every one to update the group list and install new view
			for(ActorRef p:this.group) {
				p.tell(update, null);
			}
		}
	}
	
	private void onCreateNewActor(CreateNewActor msg) {
		if(this.id==0) {
			//creating new participant
			ActorRef newP= getContext().system().actorOf(Participant.props(this.group.size()), "participant" +this.group.size());
			List<ActorRef> temp = new ArrayList<>();
			
			temp.addAll(this.group);
			temp.add(newP);
			newP.tell(new JoinGroupMsg(temp,this.view+1), getSelf());
			
			JoinGroupMsg join = new JoinGroupMsg(temp,this.view);
			for (ActorRef peer: group) {
				peer.tell(join, getSelf());
			}
			
			JoinNew update = new JoinNew(temp);
			getSelf().tell(update, getSelf());
		}
	}
	
	private void onJoinNew(JoinNew msg) {
		if(this.id==0) {
			//PrintNewViewMsg(msg.group);
			ViewChange update = new ViewChange(msg.group,this.view);
			//tell every one to update the group list and install new view
			for(ActorRef p:msg.group) {
				p.tell(update, null);
			}
		}
	}
}
