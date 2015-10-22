package com.allen.rabbitmq.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ErrorHandler;

public class MessageErrorHandler implements ErrorHandler {

	private static final Logger logger = LoggerFactory.getLogger(MessageErrorHandler.class);
	public void handleError(Throwable t) {
		// TODO Auto-generated method stub
		logger.error("RabbitMQ happen a error: " + t.getMessage(),t);
	}

}
