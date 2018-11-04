package virtualSynchrony;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;
import virtualSynchrony.Participant.*;

public class VSMain {
	
	final private static int N_PARTICIPANTS = 5;

	public static void main(String[] args) throws InterruptedException {
		final ActorSystem system = ActorSystem.create("helloakka");
		Scanner scanner = new Scanner(System.in);
	    String option = null;
	    boolean sendStable;
	    
		
		// actors list
		List<ActorRef> group = new ArrayList<>();
		int id=0;
		
		// creating and adding GM in actors list
		group.add(system.actorOf(Participant.props(id), "GM"+ id));
		
		// creating and adding other participants in the list
		
		while(id<N_PARTICIPANTS) {
			id++;
			group.add(system.actorOf(Participant.props(id), "participant" +id ));
		}
		
		// send the group member list to everyone in the group 
		for (ActorRef peer: group) {
			if(!peer.equals(group.get(0))) 
				peer.tell(new JoinGroupMsg(Collections.unmodifiableList(group),0), null);
			else 
				peer.tell(new JoinGroupMsg(group,0), null);
		}
		
		

	    ShowMenu();
	    option = scanner.nextLine();

	    switch(option) {
	    case "1" :
	    	System.out.println("option 1 selected");
	    	sendStable=true; //also send the stable msg
	    	for(ActorRef p: group) {
	    		if(!p.path().name().equals("GM0")) {
	    			p.tell(new StartChatMsg(sendStable), null);
	    			p.tell(new StartChatMsg(sendStable), null);
	    		}
	    	}
	    	break;
	    	
	    case "2" :
	    	//2: add new participent after P1 stable multicast
	    	System.out.println("option 2 selected");
	    	sendStable=true;
	    	// tell p1 to start chat msg
	    	group.get(1).tell(new StartChatMsg(sendStable), null);
	    	//this time must be grater than 300ms as we are using 200ms for unstable timeout and 10ms for each msg
	    	// tell GM to create new actor
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(310, TimeUnit.MILLISECONDS),  
	    	          group.get(0),
	    	          new CreateNewActor(), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	
	    	//this time must be grater than 1500ms as we are using 1500ms as flush timeout
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(1820, TimeUnit.MILLISECONDS),  
	    	          group.get(2),
	    	          new StartChatMsg(sendStable), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	
	    	break;
	    	
	    case "3" :
	    	//3: crash P1 after stable multicast
	    	System.out.println("option 3 selected");
	    	sendStable=true;
	    	group.get(1).tell(new StartChatMsg(sendStable), null);
	    	//System.out.println(group.size());
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(310, TimeUnit.MILLISECONDS),  
	    	          group.get(0),
	    	          new ParticipantCrashed(group.get(1)), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	//this time must be grater than 1500ms as we are using 1500ms as flush timeout
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(1820, TimeUnit.MILLISECONDS),  
	    	          group.get(2),
	    	          new StartChatMsg(sendStable), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	break;
	    	
	    case "4" :
	    	//4: crash P1 after unstable multicast
	    	System.out.println("option 4 selected");
	    	sendStable=false;
	    	group.get(1).tell(new StartChatMsg(sendStable), null);
	    	break;
	    	
	    case "5" :
	    	//5: crash P2 after receiving multicast
	    	System.out.println("option 5 selected");
	    	group.get(2).tell(new CrashAfterReceiveMulticast(true), null);
	    	sendStable=true;
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(10, TimeUnit.MILLISECONDS),  
	    	          group.get(1),
	    	          new StartChatMsg(sendStable), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	break;
	    	
	    case "6" :
	    	//6: crash P3 after receiving view change msg
	    	// crashing and adding new participant is happening in the same install view
	    	System.out.println("option 6 selected");
	    	group.get(3).tell(new CrashAfterViewChange(true), null);
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(10, TimeUnit.MILLISECONDS),  
	    	          group.get(0),
	    	          new CreateNewActor(), // the message to send
	    	          system.dispatcher(), null
	    	          );
	    	
	    	sendStable=true;
	    	system.scheduler().scheduleOnce(
	    	          Duration.create(1520, TimeUnit.MILLISECONDS),  
	    	          group.get(1),
	    	          new StartChatMsg(sendStable), // the message to send
	    	          system.dispatcher(), null
	    	          );
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
	    try {
			Runtime.getRuntime().exec("Rscript check.r");
		} catch (IOException e) {} 
	      system.terminate();
	  }

	private static void ShowMenu() {
		System.out.println("1: normal multicast");
		System.out.println("2: add new participent after P1 stable multicast");
		System.out.println("3: crash P1 after stable multicast");
		System.out.println("4: crash P1 after unstable multicast");
		System.out.println("5: crash P2 after receiving multicast");
		System.out.println("6: Create new Actor, crash P3 at view change");
		System.out.println("SELECT THE OPTION");
		
	}
}
