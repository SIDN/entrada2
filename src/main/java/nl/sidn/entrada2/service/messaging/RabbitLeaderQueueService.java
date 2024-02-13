package nl.sidn.entrada2.service.messaging;

import org.apache.iceberg.DataFile;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada2.service.IcebergService;
import nl.sidn.entrada2.service.LeaderService;
import nl.sidn.entrada2.util.ConditionalOnRabbitMQ;

@ConditionalOnRabbitMQ
@Service
@Slf4j
public class RabbitLeaderQueueService extends AbstractRabbitQueue implements LeaderQueue{

	@Value("${entrada.messaging.leader.name}")
	private String queueName;

	@Autowired(required = false)
	@Qualifier("rabbitByteTemplate")
	private AmqpTemplate rabbitTemplate;

	@Autowired
	private IcebergService icebergService;

	@Autowired
	private LeaderService leaderService;

	@RabbitListener(id = "${entrada.messaging.leader.name}", queues = "#{leaderQueue.name}", containerFactory = "#{rabbitListenerByteContainerFactory}", autoStartup = "false")
	public void onMessage(DataFile message) {
		log.info("Received RabbitMQ message, rows: {} path: {}", message.recordCount(), message.path());

		if (leaderService.isleader()) {
			icebergService.commit(message);
		}
	}

	public void send(DataFile message) {
		log.info("Send new file to commit to leader queue, file : " + message.path());
		
		rabbitTemplate.convertAndSend(queueName + "-exchange", queueName, message);
	}

	@Override
	public String name() {
		return queueName;
	}

}
