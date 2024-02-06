package nl.sidn.entrada2.service;

import org.apache.iceberg.DataFile;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.rabbitmq.client.Channel;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LeaderQueueService extends AbstractQueueService {

	@Value("${entrada.messaging.leader-queue}")
	private String queueName;

	@Autowired(required = false)
	@Qualifier("rabbitByteTemplate")
	private AmqpTemplate rabbitTemplate;

	@Autowired
	private IcebergService icebergService;

	@Autowired
	private LeaderService leaderService;

	@ConditionalOnProperty(prefix = "spring.rabbitmq", name = "enabled", havingValue = "true")
	@RabbitListener(id = "${entrada.messaging.leader-queue}", queues = "#{leaderQueue.name}",
	containerFactory = "#{rabbitListenerByteContainerFactory}", autoStartup = "false")
	public void receiveMessageManual(DataFile message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

		handleMessage(message);

		// channel.basicAck(tag, false);

	}

	@ConditionalOnProperty(prefix = "spring.cloud.aws.sqs", name = "enabled", havingValue = "true")
	@SqsListener("${entrada.messaging.command-queue}-queue.fifo")
	public void receiveMessage(DataFile message, @Header("SenderId") String senderId) {
		
		handleMessage(message);
	}
	
	
	private void handleMessage(DataFile message) {
		log.info("Received RabbitMQ message, rows: {} path: {}", message.recordCount(), message.path());
		
		if (leaderService.isleader()) {
			icebergService.commit(message);
		}
		
	}

	public void send(DataFile message) {

		if (awsEnabled) {
			// TODO
		} else {
			rabbitTemplate.convertAndSend(queueName + "-exchange", queueName, message);
		}
	}

	@Override
	public String name() {
		return queueName;
	}

}
