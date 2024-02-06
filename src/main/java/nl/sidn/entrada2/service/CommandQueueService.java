package nl.sidn.entrada2.service;

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
import nl.sidn.entrada2.messaging.Command;
import nl.sidn.entrada2.messaging.Command.CommandType;
import nl.sidn.entrada2.service.StateService.APP_STATE;

@Service
@Slf4j
public class CommandQueueService extends AbstractQueueService {
	
	@Value("${entrada.messaging.command-queue}")
	private String queueName;
	@Autowired
	private RequestQueueService requestQueueService;
	@Autowired
	private StateService stateService;
	
	@Autowired(required = false)
	@Qualifier("rabbitCommandTemplate")
    private AmqpTemplate rabbitTemplate;

	@Autowired
	private WorkService workService;
	
	@ConditionalOnProperty(prefix = "spring.rabbitmq", name = "enabled", havingValue = "true")
	@RabbitListener(id = "${entrada.messaging.command-queue}", queues = "#{commandQueue.name}")
	public void receiveMessageManual(Command message, Channel channel,
			@Header(AmqpHeaders.DELIVERY_TAG) long tag){
		
		handleMessage(message);
		// channel.basicAck(tag, false);

	}

	@ConditionalOnProperty(prefix = "spring.cloud.aws.sqs", name = "enabled", havingValue = "true")
	@SqsListener("${entrada.messaging.command-queue}-queue.fifo")
	public void receiveMessage(Command message, @Header("SenderId") String senderId) {

		handleMessage(message);
	}
	
	
	private void handleMessage(Command message) {
		log.info("Received command message: {}", message);

		if(message.getCommand() == CommandType.START) {
			requestQueueService.start();
			stateService.setState(APP_STATE.RUNNING);
		}else if(message.getCommand() == CommandType.STOP) {
			requestQueueService.stop();
			workService.stop();
			stateService.setState(APP_STATE.STOPPED);
		}else {
			log.error("Unknown command, ignoring");
		}
	}

    public void send(Command message) {
    	log.info("Sending Message to the Queue : " + message);
    	if(awsEnabled) {
    		//TODO
    	}else {
    		 rabbitTemplate.convertAndSend(queueName + "-exchange", queueName, message);
    	} 
    }

	@Override
	public String name() {
		return queueName;
	}
	


}
