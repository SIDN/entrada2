package nl.sidn.entrada2.messaging;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class Command {
	
	public enum CommandType {
		START,
		STOP
	}
	
	public CommandType command;

	public Command(CommandType command) {
		this.command = command;
	}

}
