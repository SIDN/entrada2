package nl.sidn.entrada2.service.messaging;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.service.CommandService;
import nl.sidn.entrada2.util.ConditionalOnRabbitMQ;

@ConditionalOnRabbitMQ
@Service
@Slf4j
public class RabbitCommandQueueService extends AbstractRabbitQueue implements CommandQueue {

	@Value("${entrada.messaging.command.name}")
	private String queueName;
	@Autowired
	private CommandService commandService;

	@Autowired(required = false)
	@Qualifier("rabbitJsonTemplate")
	private AmqpTemplate rabbitTemplate;

	@RabbitListener(id = "${entrada.messaging.command.name}", queues = "#{commandQueue.name}")
	public void receiveMessageManual(Command message) {
		log.info("Received command message: {}", message);

		commandService.execute(message);
	}

	public void send(Command message) {
		log.info("Sending command message: {}", message);
		rabbitTemplate.convertAndSend(queueName + "-exchange", queueName, message);
	}

	@Override
	public String name() {
		return queueName;
	}

}
