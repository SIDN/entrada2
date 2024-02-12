package nl.sidn.entrada2.service.messaging;

import org.apache.iceberg.DataFile;

public interface LeaderQueue extends QueueService{
	
	public void send(DataFile message);

}
