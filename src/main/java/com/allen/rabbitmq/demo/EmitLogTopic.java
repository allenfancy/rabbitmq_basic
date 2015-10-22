package com.allen.rabbitmq.demo;

import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {

	private static final String EXCHANGE_NAME = "topic_logs";
	
	public static void main(String[] agrs) throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		
		String[] routing_keys = new String[]{"kernel.info","cron.warning","auth.info","kernel.critical","shell.debug"};
		
		for(String routing_key : routing_keys){
			String msg = UUID.randomUUID().toString();
			channel.basicPublish(EXCHANGE_NAME, routing_key, null, msg.getBytes());
			System.out.println(" [x] Sent routingKey = "+routing_key+" ,msg = " + msg + ".");
		}
		channel.close();
		connection.close();
		
	}
}
