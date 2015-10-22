package com.allen.rabbitmq.rabbitmq;

import java.util.Map;

public interface EventController {

	/**
	 * 控制器启动方法
	 */
	void start();
	/**
	 * 获取发送模板
	 * @return
	 */
	EventTemplate getEopEventTemplate();
	/**
	 * 绑定消费程序到对引发的exchange和queue
	 * @param queueName
	 * @param exchangeName
	 * @param eventProcesser
	 * @return
	 */
	EventController add(String queueName,String exchangeName,EventProcesser eventProcesser);
	/**
	 * in map,the key is queue name,but value is exchage name
	 * @param bindings
	 * @param eventProcesser
	 * @return
	 */
	EventController add(Map<String,String> bindings,EventProcesser eventProcesser);
	
}
