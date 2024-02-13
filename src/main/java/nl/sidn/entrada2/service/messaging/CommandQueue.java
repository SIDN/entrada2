package nl.sidn.entrada2.service.messaging;

import nl.sidn.entrada2.messaging.Command;

public interface CommandQueue extends Queue{
	
	public void send(Command message);

}
