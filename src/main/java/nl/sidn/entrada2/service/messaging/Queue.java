package nl.sidn.entrada2.service.messaging;

public interface Queue {
	
	public void start();
	
	public void stop();
	
	public String name();

}
