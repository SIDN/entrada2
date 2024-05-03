package nl.sidn.entrada2.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import nl.sidn.entrada2.service.StateService;
import nl.sidn.entrada2.service.StateService.APP_STATE;

@RestController
@RequestMapping("${entrada.api.context-path}/state")
public class StateController {

	@Autowired
	private StateService stateService;

	@PutMapping(path = "/start")
	public ResponseEntity<Void> start() {

		stateService.start();

		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PutMapping(path = "/stop")
	public ResponseEntity<Void> stop() {

		stateService.stop();

		return new ResponseEntity<>(HttpStatus.OK);
	}

	@GetMapping
	public ResponseEntity<APP_STATE> state() {
		return new ResponseEntity<>(stateService.getState(), HttpStatus.OK);
	}
}
