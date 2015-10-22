package com.allen.rabbitmq.rabbitmq;
/**
 * 
 * @author allen
 *新增一个接口专门来实现发送功能
 */
public interface EventTemplate {

	void send(String queueName,String exchangeName,Object eventContent) throws SendRefuseException;
	void send(String queueName,String exchangeName,Object eventContent,CodecFactory codecFactory) throws SendRefuseException;
}
