package nl.sidn.entrada2.service.messaging;

import java.util.List;

import org.apache.iceberg.DataFile;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
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

	private final AmqpTemplate rabbitTemplate;
	private final IcebergService icebergService;
	private final LeaderService leaderService;

	public RabbitLeaderQueueService(RabbitListenerEndpointRegistry listenerRegistry, @Nullable @Qualifier("rabbitByteTemplate") AmqpTemplate rabbitTemplate, IcebergService icebergService, LeaderService leaderService) {
		super(listenerRegistry);
		this.rabbitTemplate = rabbitTemplate;
		this.icebergService = icebergService;
		this.leaderService = leaderService;
	}

	@RabbitListener(id = "${entrada.messaging.leader.name}", queues = "#{leaderQueue.name}", containerFactory = "#{rabbitListenerByteContainerFactory}", autoStartup = "false")
	public void onMessage(List<DataFile> messages) {

		if (leaderService.isleader()) {
			
			log.info("Received {} Iceberg append files", messages.size());
			
			icebergService.commit(messages);
		}
	}

	public void send(DataFile message) {
		log.info("Send new file to commit to leader queue, file : " + message.location());
		
		rabbitTemplate.convertAndSend(queueName + "-exchange", queueName, message);
	}

	@Override
	public String name() {
		return queueName;
	}

}
