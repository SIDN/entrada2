package nl.sidn.entrada2.config;

import java.util.Arrays;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

import com.fasterxml.jackson.databind.ObjectMapper;

import nl.sidn.entrada2.util.ConditionalOnRabbitMQ;

@Configuration
@ConditionalOnRabbitMQ
public class RabbitMqConfig {
	
	@Value("${entrada.messaging.request.name}")
	private String requestQueue;

	@Value("${entrada.messaging.command.name}")
	private String commandQueue;
	
	@Value("${entrada.messaging.leader.name}")
	private String leaderQueue;

	@Value("${spring.rabbitmq.retry-attempts}")
	private int retryAttempts;
	
	@Value("${spring.rabbitmq.backoff-interval}")
	private int backoffInterval;
	
	@Value("${spring.rabbitmq.backoff-multiplier}")
	private int backoffMultiplier;
	
	@Value("${spring.rabbitmq.backoff-max-interval}")
	private int backoffMaxInterval;

    @Bean
    public Queue requestQueue(){
        return QueueBuilder
                .durable(requestQueue + "-queue")
                .build();
    }
    
    @Bean
    public Queue leaderQueue(){
        return QueueBuilder
                .durable(leaderQueue + "-queue")
                .build();
    }
    
    @Bean
    public Queue commandQueue(){
        return QueueBuilder
        		.nonDurable()
        		.autoDelete()
                .build();
    }
    
    @Bean
    public Exchange requestExchange(){
        return new DirectExchange(requestQueue + "-exchange", true, false);
    }
    
    @Bean
    public Exchange leaderExchange(){
        return new DirectExchange(leaderQueue + "-exchange", true, false);
    }
    
    @Bean
    public Exchange commandExchange(){
        return new FanoutExchange(commandQueue + "-exchange", true, false);
    }

    @Bean
    public Binding bindRequestQueue(){
        return BindingBuilder
                .bind(requestQueue())
                .to(requestExchange())
                .with(requestQueue)
                .noargs();
    }
    
    @Bean
    public Binding bindLeaderQueue(){
        return BindingBuilder
                .bind(leaderQueue())
                .to(leaderExchange())
                .with(leaderQueue)
                .noargs();
    }
    
    @Bean
    public Binding bindCommandQueue(){
        return BindingBuilder
                .bind(commandQueue())
                .to(commandExchange())
                .with(commandQueue)
                .noargs();
    }


    @Bean
    public MessageConverter messageConverter(ObjectMapper mapper){
    	Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(mapper);
        converter.setCreateMessageIds(true); //create a unique message id for every message
        return converter;
    }

    /**
     * 
     * Create template for sending json to queue
     */
    @Bean
    public RabbitTemplate rabbitJsonTemplate(ConnectionFactory factory, ObjectMapper objectMapper){
        RabbitTemplate template = new RabbitTemplate();
        template.setConnectionFactory(factory);
        template.setMessageConverter(messageConverter(objectMapper));
        return template;
    }
    
    /**
     * 
     * Create template for sending non-json (serialized objects) to queue
     */
    @Bean
    public RabbitTemplate rabbitByteTemplate(ConnectionFactory factory, ObjectMapper objectMapper){
        RabbitTemplate template = new RabbitTemplate();
        template.setConnectionFactory(factory);
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            RetryOperationsInterceptor retryInterceptor,
            ObjectMapper objectMapper) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter(objectMapper));
        factory.setPrefetchCount(0);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setAdviceChain(retryInterceptor);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        return factory;
    }

    /** 
     * simple convertor for sending over Iceberg data files.
     */
    @Bean
    public MessageConverter simpleMessageConverter() {
        SimpleMessageConverter convertor = new SimpleMessageConverter();
        convertor.setAllowedListPatterns(Arrays.asList("org.apache.iceberg.*", "java.*"));
        return convertor;
    }
    
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerByteContainerFactory(
            ConnectionFactory connectionFactory,
            RetryOperationsInterceptor retryInterceptor,
            ObjectMapper objectMapper) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(simpleMessageConverter());
        factory.setPrefetchCount(0);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setAdviceChain(retryInterceptor);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        return factory;
    }
    
    
    @Bean
    public RetryOperationsInterceptor retryInterceptor(){
        return RetryInterceptorBuilder.stateless().maxAttempts(3)
                .backOffOptions(backoffInterval, backoffMultiplier, backoffMaxInterval)
                .maxAttempts(retryAttempts)
                .build();
    }

}