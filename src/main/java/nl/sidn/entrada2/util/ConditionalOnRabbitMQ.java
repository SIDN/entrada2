package nl.sidn.entrada2.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ConditionalOnExpression(
	    "T(org.apache.commons.lang3.StringUtils).isNotEmpty('${spring.rabbitmq.host:}')"
	)
public @interface ConditionalOnRabbitMQ {

}