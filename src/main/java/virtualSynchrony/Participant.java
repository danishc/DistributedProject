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
import akka.actor.Cancellable;
import akka.actor.Props;

public class Participant extends AbstractActor {
	
	private final int id;
	
	private List<ActorRef> group;
	private Random rnd = new Random();
	private int sendCount =0;
	final static int N_MESSAGES = 100;							// max number of msgs an actor can multicast
	private boolean crashAfterReceiveMulticast=false; 			// for condition number 5
	private boolean crashAfterViewChange=false;					// check for condition number 6
	private List<ActorRef> flushGroup = new ArrayList<>();
	private List<ChatMsg> buffer =new ArrayList<>();
	protected boolean crashed = false;          				// simulates a crash
	private Cancellable timerMulticast;							// stop timer when inhabit_sent equal to zero
	private int inhibit_sends = 0;								// allow multicast only when equal to zero
	private int view = 0;										// view installed by current actor
	  
	
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
	
	public static class JoinNew implements Serializable {}
	
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
		private final int view;

		public FlushTimeout(int view) {
			this.view= view;
		}
	}
	
	public static class ViewChange implements Serializable {
		private final int view;

		public ViewChange(int i) {
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
	
	private void onCrashAfterReceiveMulticast(CrashAfterReceiveMulticast msg) {
		this.crashAfterReceiveMulticast = msg.crashAfterReceiveMulticast;
	}
	
	private void onCrashAfterViewChange(CrashAfterViewChange msg) {
		this.crashAfterViewChange = msg.crashAfterViewChange;
	}
	
	private void onJoinGroupMsg(JoinGroupMsg msg) {
		if(this.id==0) {
			this.group = msg.group;
		}
		else {
			this.group = Collections.unmodifiableList(msg.group);
		}
		
		
		if(this.view != msg.epoch) {
			
			this.view = msg.epoch;
		}
		//PrintNewViewMsg(this.group);
	}
	
	private void onAllowMulticast(AllowMulticast msg) {
		if(this.inhibit_sends==0) {
			this.timerMulticast.cancel();
			getSelf().tell(new StartChatMsg(msg.sendStable),getSelf());
		}
	}
	
	private void onStartChatMsg(StartChatMsg msg) {
		if(sendCount<N_MESSAGES) {
			if(this.inhibit_sends==0) { //when inhabit_sends is equal to zero it means we are allowed to multicast a msg
			
				sendCount++; //number of messages broadcast by a single actor
				ChatMsg um = new ChatMsg(sendCount,this.id, false, false, this.view); //unstable msg
				ChatMsg sm = new ChatMsg(sendCount,this.id, true, false, this.view); //stable msg
			
				System.out.println(this.id +" send multicast m"+ sendCount + " within " + this.view);
				if(multicast(um,this.group) && msg.sendStable) {
					//multicast stable message
					multicast(sm,this.group);
				}
			}
			else {
				//System.out.println("inhabit is grater the 1");
				//wait for inhibit_sends to turn to zero
				timerMulticast =getContext().system().scheduler().schedule(
						Duration.create(50, TimeUnit.MILLISECONDS),
		    			Duration.create(100, TimeUnit.MILLISECONDS),  
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
        	group.get(0).tell(new ParticipantCrashed(group.get(msg.senderid)), getSelf());
    	}
    			 
	}

    private void onFlushTimeout(FlushTimeout msg) throws InterruptedException {
    	int groupSizeDiff =this.group.size()-this.flushGroup.size();
    	if(isAllFlush(this.group)) {	//install new view successfully
    		this.view=msg.view;
        	this.inhibit_sends=0;
    		PrintNewViewMsg(this.group);
    		//Any message that was sent in view and has not yet been delivered may be discarded
    		this.buffer.clear();
    	}
    	//after time out if there is still someone who did not send the flush
    	else if(groupSizeDiff>=1) {	// some one crashed while sending flush
    		List<ActorRef> tmp= new ArrayList<>();
    		tmp.addAll(this.group);
    		tmp.removeAll(this.flushGroup);
 		
    		for(int i=0; i<tmp.size(); i++) {
    			getContext().system().scheduler().scheduleOnce(
    	      	          Duration.create(1000, TimeUnit.MILLISECONDS),  
    	      	          getSelf(),	//tell GM that p crashed
    	      	          new ParticipantCrashed(tmp.get(i)), // the message to send
    	      	          getContext().system().dispatcher(), getSelf()
    	      	          );
    		}
    	}
    }
    
	private void onViewChange(ViewChange msg) {					/* View changed*/
		if(!crashAfterViewChange) {	// check for condition number 6
			//1) stop multicast
			this.inhibit_sends++;
		
			if(!this.buffer.isEmpty()) {	//check if buffer is not empty mean there is some unstable msg received by current participant.
				for(ChatMsg tmp : this.buffer) {
					// 2) multicast unstable msg
					ChatMsg m= new ChatMsg(tmp.n,tmp.senderId,true,false,tmp.view);
					multicast(m,this.group); 			
			    }
			}
			this.flushGroup.clear();
			this.flushGroup.add(getSelf());
		
			// 3) multicast flush to every one
			ChatMsg m= new ChatMsg(0,this.id,false,true,msg.view);
			if(multicast(m,this.group))		{	//wait for multicast to complete
				getContext().system().scheduler().scheduleOnce(
						Duration.create(500, TimeUnit.MILLISECONDS),  
						getSelf(),
						new FlushTimeout(msg.view+1), // on flush time out it check if it received flush messages from every one
						getContext().system().dispatcher(), getSelf()
				);
    		}
		}
	}
	
	private void deliver(ChatMsg m)  {
		// for unstable messages
		if(!m.isStable && !m.isFlush) {
			
			this.buffer.add(m);
			//set timeout, on timeout it will check if stable msg received else it calls crashDetected method 
			getContext().system().scheduler().scheduleOnce(
			          Duration.create(200, TimeUnit.MILLISECONDS),  
			          getSelf(),
			          new Timeout(m.senderId), // the message to send
			          getContext().system().dispatcher(), getSelf()
			          );
			//System.out.println(this.id+ " recived uM");
			
		}
		
		else if(m.isFlush) {	//if msg is of flush type
			this.flushGroup.add(getSender()); // add sender in the flushGroup
			
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
			if(crashAfterReceiveMulticast) { // for condition number 5
				group.get(0).tell(new ParticipantCrashed(getSelf()), null);
			}
		}
	    
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
	
	private void PrintNewViewMsg(List<ActorRef> group2) {
		StringBuffer tmp= new StringBuffer();
		for(ActorRef p: group2) {
			if(!p.path().name().equals("GM0"))
				tmp.append(",");
			tmp.append(p.path().name().replaceAll("\\D+",""));
		}
		System.out.println(this.id+ " install view "+this.view+" "+ tmp.toString());
		
	}
	
	private boolean isAllFlush(List<ActorRef> group) {
		return new HashSet<>(this.group).equals(new HashSet<>(this.flushGroup));
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
		
		if(group.contains(msg.crashActor) && this.id==0) {
			this.group.remove(msg.crashActor);	//removing crashed participant from the group
			ViewChange update = new ViewChange(this.view);
			
			for(ActorRef p:this.group) {	//tell every one to update the group list and install new view
				p.tell(update, null);
			}
		}
	}
	
	private void onCreateNewActor(CreateNewActor msg) throws InterruptedException {
		if(this.id==0) { // make sure that only GM can execute this code
			//creating new participant
			ActorRef newP= getContext().system().actorOf(Participant.props(this.group.size()), "participant" +this.group.size());
			this.group.add(newP); //GM updating its group list
			newP.tell(new JoinGroupMsg(Collections.unmodifiableList(this.group),this.view), getSelf());
			
			//using scheduler so new participant can update its group list before receiving view change request
			getContext().system().scheduler().scheduleOnce(
	      	          Duration.create(100, TimeUnit.MILLISECONDS),  
	      	          getSelf(),	
	      	          new JoinNew(), // the message to send
	      	          getContext().system().dispatcher(), getSelf()
	      	          );
		}
	}
	
	private void onJoinNew(JoinNew msg) {
		if(this.id==0) {
			ViewChange update = new ViewChange(this.view);
			//tell every one to install new view
			for(ActorRef p:this.group) {
				p.tell(update, null);
			}
		}
	}
}
