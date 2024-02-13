package nl.sidn.entrada2.service.messaging;

import org.apache.iceberg.DataFile;

public interface LeaderQueue extends Queue{
	
	public void send(DataFile message);

}
