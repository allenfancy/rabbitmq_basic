package com.allen.rabbitmq.demo;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogDirect {

	private static final String EXCHANGE_NAME = "ex_logs_direct";
	private static final String[] SEVERITIES = {"info","warning","error"};
	
	public static void main(String[] args) throws IOException{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();	
		
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		
		for(int i = 0; i < 6;i++){
			String severity = getSeverity();
			String message = severity + "_log:" + UUID.randomUUID().toString();
			//发布消息至转发器，指定routingKey
			channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
			System.out.println("[x] Sent : " + message + ".");
		}
		channel.close();
		connection.close();
			
				
	}

	private static String getSeverity() {
		// TODO Auto-generated method stub
		Random random = new Random();
		int ranVal = random.nextInt(3);
		return SEVERITIES[ranVal];
	}
}
