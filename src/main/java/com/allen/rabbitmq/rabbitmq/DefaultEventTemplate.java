package com.allen.rabbitmq.rabbitmq;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;

public class DefaultEventTemplate  implements EventTemplate{

	private static final Logger logger = LoggerFactory.getLogger(DefaultEventTemplate.class);

	private AmqpTemplate eventAmqpTemplate;

	private CodecFactory defaultCodecFactory;

	private DefaultEventController eec;

	public DefaultEventTemplate(AmqpTemplate eopAmqpTemplate,
			CodecFactory defaultCodecFactory, DefaultEventController eec) {
		this.eventAmqpTemplate = eopAmqpTemplate;
		this.defaultCodecFactory = defaultCodecFactory;
		this.eec = eec;
	}
	
	public DefaultEventTemplate(AmqpTemplate eopAmqpTemplate,CodecFactory defaultCodecFactory) {
		this.eventAmqpTemplate = eopAmqpTemplate;
		this.defaultCodecFactory = defaultCodecFactory;
	}
	public void send(String queueName, String exchangeName, Object eventContent)
			throws SendRefuseException {
		this.send(queueName, exchangeName, eventContent, defaultCodecFactory);
	}  
	public void send(String queueName, String exchangeName, Object eventContent,
			CodecFactory codecFactory) throws SendRefuseException {
		if (StringUtils.isEmpty(queueName) || StringUtils.isEmpty(exchangeName)) {
			throw new SendRefuseException("queueName exchangeName can not be empty.");
		}
		
		if (!eec.beBinded(exchangeName, queueName))
			eec.declareBinding(exchangeName, queueName);

		byte[] eventContentBytes = null;
		if (codecFactory == null) {
			if (eventContent == null) {
				logger.warn("Find eventContent is null,are you sure...");
			} else {
				throw new SendRefuseException(
						"codecFactory must not be null ,unless eventContent is null");
			}
		} else {
			try {
				eventContentBytes = codecFactory.serialize(eventContent);
			} catch (IOException e) {
				throw new SendRefuseException(e);
			}
		}

		// 构造成Message
		EventMessage msg = new EventMessage(queueName, exchangeName,
				eventContentBytes);
		try {
			eventAmqpTemplate.convertAndSend(exchangeName, queueName, msg);
		} catch (AmqpException e) {
			logger.error("send event fail. Event Message : [" + eventContent + "]", e);
			throw new SendRefuseException("send event fail", e);
		}
	}
}
