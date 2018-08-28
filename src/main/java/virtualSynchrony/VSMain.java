package virtualSynchrony;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import virtualSynchrony.Participant.JoinGroupMsg;
import virtualSynchrony.Participant.PrintHistoryMsg;
import virtualSynchrony.Participant.StartChatMsg;
import virtualSynchrony.Participant.ViewChange;
import virtualSynchrony.Participant.JoinNewAfterMulticast;

public class VSMain {
	
	final private static int N_PARTICIPANTS = 5;

	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("helloakka");
		Scanner scanner = new Scanner(System.in);
	    String option = null;
		
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
		
		

	    ShowMenu();
	    option = scanner.nextLine();

	    switch(option) {
	    case "1" :
	    	System.out.println("option 1 selected");
	    	for(int j=1; j<=N_PARTICIPANTS; j++) {
	    		group.get(j).tell(new StartChatMsg(), null);
	    	}
	    	break;
	    	
	    case "2" :
	    	System.out.println("option 2 selected");
	    	// tell p1 to start chat msg
	    	group.get(1).tell(new StartChatMsg(), null);
	    	// tell GM to install new view after unstable msg received
	    	group.get(0).tell(new JoinNewAfterMulticast(true), null);
	    	break;
	    default :
	        System.out.println("Invalid Option");
	    }
	    

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

	private static void ShowMenu() {
		System.out.println("1: normal multicast");
		System.out.println("2: add new participent after unstable/stable multicast");
		System.out.println("SELECT THE OPTION");
		
	}
}
