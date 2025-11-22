package nl.sidn.entrada2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.messaging.Command.CommandType;
import nl.sidn.entrada2.service.messaging.CommandQueue;

@Service
@Data
@Slf4j
public class StateService {

	public enum APP_STATE {
		ACTIVE, STOPPED
	};

	private APP_STATE state = APP_STATE.ACTIVE;

	@Getter(AccessLevel.NONE)
	@Setter(AccessLevel.NONE)
	@Autowired
	private CommandQueue commandQueue;

	/**
	 * Start processing new pcap objects from s3 location
	 */
	public void start() {
		commandQueue.send(new Command(CommandType.START));
	}
	
	/**
	 * Stop processing new pcap files from s3 location
	 */
	public void stop() {
		commandQueue.send(new Command(CommandType.STOP));
	}

	public void setState(APP_STATE state) {
		this.state = state;
		log.info("Changing state to: {}", state);
	}
	

}
