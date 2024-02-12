package nl.sidn.entrada2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PutMapping;

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
		RUNNING, STOPPED
	};

	private APP_STATE state = APP_STATE.RUNNING;

	@Getter(AccessLevel.NONE)
	@Setter(AccessLevel.NONE)
	@Autowired
	private CommandQueue commandQueue;

	public void start() {
		commandQueue.send(new Command(CommandType.START));
	}

	@PutMapping(path = "/stop")
	public void stop() {
		commandQueue.send(new Command(CommandType.STOP));
	}

	public void setState(APP_STATE state) {
		this.state = state;
		log.info("Change state to: {}", state);
	}
	

}
