package com.allen.rabbitmq.demo;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitConnection {

	private static final String HOSTS = "localhost";
	
	public static ConnectionFactory getFactory(){
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOSTS);
		return factory;
	}
	
	public static Connection getConenction() throws IOException{
		return getFactory().newConnection();
	}
	
	public static Channel getChannel() throws IOException{
		return getConenction().createChannel();
	}
}
