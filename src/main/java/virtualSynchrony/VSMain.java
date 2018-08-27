package virtualSynchrony;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import virtualSynchrony.Participant.JoinGroupMsg;
import virtualSynchrony.Participant.PrintHistoryMsg;
import virtualSynchrony.Participant.StartChatMsg;

public class VSMain {
	
	final private static int N_PARTICIPANTS = 5;

	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("helloakka");
		
		// actors list
		List<ActorRef> group = new ArrayList<>();
		int id=0;
		
		// creating and adding GM in actors list
		group.add(system.actorOf(Participant.props(id++), "GM"));
		
		// creating and adding other participants in the list
		int i=0;
		while(i<N_PARTICIPANTS) {
			i++;
			group.add(system.actorOf(Participant.props(id++), "participant" +i ));
		}
		
		// send the group member list to everyone in the group 
		JoinGroupMsg join = new JoinGroupMsg(group);
	    for (ActorRef peer: group) {
	      peer.tell(join, null);
	    }
	    
	    // tell the particioant to start conversation
	    //for (int i=1; i<=N_PARTICIPANTS; i++) {
	    //    group.get(i).tell(new StartChatMsg(), null);
	    //}
	    group.get(1).tell(new StartChatMsg(), null);
	    //group.get(2).tell(new StartChatMsg(), null);

	    try {
	        System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
	        System.in.read();
	        
	        PrintHistoryMsg msg = new PrintHistoryMsg();
	        for (ActorRef peer: group) {
	        	peer.tell(msg, null);
	        }
	        System.out.println(">>> Press ENTER to exit <<<");
	        System.in.read();
	      } 
	      catch (IOException ioe) {}
	      system.terminate();
	  }
}
