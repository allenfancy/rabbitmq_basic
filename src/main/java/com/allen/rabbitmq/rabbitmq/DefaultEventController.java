package com.allen.rabbitmq.rabbitmq;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SerializerMessageConverter;


/***
 * 
 * @author allen
 *和rabbitmq通信的控制器，主要负责：
 *<p>1.和rabbitmq建立连接</p>
 *<p>2.声明exchange和queue以及他们的绑定关系</p>
 *<p>3.启动消息监听容器，并讲不同消息处理绑定到对应的exchange和queue上</p>
 *<p>4.持有消息发送模板以及所exchange  queue和绑定关系的本地缓存</p>
 *<p></p>
 *
 */
public class DefaultEventController implements EventController{

	private CachingConnectionFactory rabbitConnectionFactory;
	private EventControlConfig config;
	private RabbitAdmin rabbitAdmin;
	private CodecFactory defaultCodecFactory = new HessionCodecFactory();
	private SimpleMessageListenerContainer msgListenerContainer;
	private MessageAdapterHandler msgAdapterHandler = new MessageAdapterHandler();
	//直接指定
	private MessageConverter serializerMessageConvertere = new SerializerMessageConverter();

	private Map<String,DirectExchange> exchanges = new HashMap<String,DirectExchange>();
	
	private Map<String,Queue> queues = new HashMap<String,Queue>();
	
	private Set<String> binded = new HashSet<String>();
	
	private EventTemplate eventTemplate;//给App使用的Event发送客户端
	
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	
	private static DefaultEventController defaultEventController;
	
	public synchronized static DefaultEventController getInstance(EventControlConfig config){
		if(defaultEventController == null){
			defaultEventController = new DefaultEventController(config);
		}
		return defaultEventController;
	}
	
	private DefaultEventController(EventControlConfig config){
		if(config == null){
			throw new IllegalArgumentException("Config can not be null.");
		}
		this.config = config;
		initRabbitConnectionFactory();
		rabbitAdmin = new RabbitAdmin(rabbitConnectionFactory);
		//初始化RabbitTemplate
		RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
		rabbitTemplate.setMessageConverter(serializerMessageConvertere);
		eventTemplate = new DefaultEventTemplate(rabbitTemplate,defaultCodecFactory,this);
	}
	
	private void initRabbitConnectionFactory(){
		rabbitConnectionFactory = new CachingConnectionFactory();
		rabbitConnectionFactory.setHost(config.getServerHost());
		rabbitConnectionFactory.setChannelCacheSize(config.getEventMsgProcessNum());
		rabbitConnectionFactory.setPort(config.getPort());
		rabbitConnectionFactory.setUsername(config.getUsername());
		rabbitConnectionFactory.setPassword(config.getPassword());
		if(!StringUtils.isEmpty(config.getVirtualHost())){
			rabbitConnectionFactory.setVirtualHost(config.getVirtualHost());
		}
	}
	
	/**
	 * 注销程序
	 */
	public synchronized void destroy() throws Exception {
		if (!isStarted.get()) {
			return;
		}
		msgListenerContainer.stop();
		eventTemplate = null;
		rabbitAdmin = null;
		rabbitConnectionFactory.destroy();
	}

	public void start() {
		if (isStarted.get()) {
			return;
		}
		Set<String> mapping = msgAdapterHandler.getAllBinding();
		for (String relation : mapping) {
			String[] relaArr = relation.split("\\|");
			declareBinding(relaArr[1], relaArr[0]);
		}
		initMsgListenerAdapter();
		isStarted.set(true);
	}
	
	/**
	 * 初始化消息监听器容器
	 */
	private void initMsgListenerAdapter(){
		MessageListener listener = new MessageListenerAdapter(msgAdapterHandler,serializerMessageConvertere);
		msgListenerContainer = new SimpleMessageListenerContainer();
		msgListenerContainer.setConnectionFactory(rabbitConnectionFactory);
		msgListenerContainer.setAcknowledgeMode(AcknowledgeMode.AUTO);
		msgListenerContainer.setMessageListener(listener);
		msgListenerContainer.setErrorHandler(new MessageErrorHandler());
		msgListenerContainer.setPrefetchCount(config.getPrefetchSize()); // 设置每个消费者消息的预取值
		msgListenerContainer.setConcurrentConsumers(config.getEventMsgProcessNum());
		msgListenerContainer.setTxSize(config.getPrefetchSize());//设置有事务时处理的消息数
		msgListenerContainer.setQueues(queues.values().toArray(new Queue[queues.size()]));
		msgListenerContainer.start();
	}

	public EventTemplate getEopEventTemplate() {
		return eventTemplate;
	}

	public EventController add(String queueName, String exchangeName,EventProcesser eventProcesser) {
		return add(queueName, exchangeName, eventProcesser, defaultCodecFactory);
	}
	
	public EventController add(String queueName, String exchangeName,EventProcesser eventProcesser,CodecFactory codecFactory) {
		msgAdapterHandler.add(queueName, exchangeName, eventProcesser, defaultCodecFactory);
		if(isStarted.get()){
			initMsgListenerAdapter();
		}
		return this;
	}

	public EventController add(Map<String, String> bindings,
			EventProcesser eventProcesser) {
		return add(bindings, eventProcesser,defaultCodecFactory);
	}

	public EventController add(Map<String, String> bindings,
			EventProcesser eventProcesser, CodecFactory codecFactory) {
		for(Map.Entry<String, String> item: bindings.entrySet()) 
			msgAdapterHandler.add(item.getKey(),item.getValue(), eventProcesser,codecFactory);
		return this;
	}
	
	/**
	 * exchange和queue是否已经绑定
	 */
	protected boolean beBinded(String exchangeName, String queueName) {
		return binded.contains(exchangeName+"|"+queueName);
	}
	
	/**
	 * 声明exchange和queue已经它们的绑定关系
	 */
	protected synchronized void declareBinding(String exchangeName, String queueName) {
		String bindRelation = exchangeName+"|"+queueName;
		if (binded.contains(bindRelation)) return;
		
		boolean needBinding = false;
		DirectExchange directExchange = exchanges.get(exchangeName);
		if(directExchange == null) {
			directExchange = new DirectExchange(exchangeName, true, false, null);
			exchanges.put(exchangeName, directExchange);
			rabbitAdmin.declareExchange(directExchange);//声明exchange
			needBinding = true;
		}
		
		Queue queue = queues.get(queueName);
		if(queue == null) {
			queue = new Queue(queueName, true, false, false);
			queues.put(queueName, queue);
			rabbitAdmin.declareQueue(queue);	//声明queue
			needBinding = true;
		}
		
		if(needBinding) {
			Binding binding = BindingBuilder.bind(queue).to(directExchange).with(queueName);//将queue绑定到exchange
			rabbitAdmin.declareBinding(binding);//声明绑定关系
			binded.add(bindRelation);
		}
	}

}
