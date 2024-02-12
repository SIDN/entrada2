package nl.sidn.entrada2.service.messaging;

import nl.sidn.entrada2.messaging.Command;

public interface CommandQueue {
	
	public void send(Command message);

}
