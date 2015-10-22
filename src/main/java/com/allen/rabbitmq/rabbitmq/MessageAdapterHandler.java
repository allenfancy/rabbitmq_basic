package com.allen.rabbitmq.rabbitmq;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 
 * @author allen
 *         <p>
 *         消息处理配置器，主要功能：
 *         </p>
 *         <p>
 *         将不同的消息类型绑定到对已ing的处理器并本地缓存，如将queue01+exchange01的消息同一交由A处理器处理
 *         </p>
 *         <p>
 *         执行消息的消费分发，调用相应的处理器来消费属于它的消息
 *         </p>
 */
public class MessageAdapterHandler {

	private static final Logger logger = LoggerFactory.getLogger(MessageAdapterHandler.class);
	private ConcurrentMap<String, EventProcessorWrap> epwMap;

	public MessageAdapterHandler() {
		this.epwMap = new ConcurrentHashMap<String, EventProcessorWrap>();
	}

	public void hanleMessage(EventMessage eem) {
		logger.debug("Receive an EventMessage ：[" + eem + "]");
		if (eem == null) {
			logger.warn("Receive an null EventMessage,it may product some errors,and processing message is canceled.");
			return;
		}
		if (StringUtils.isEmpty(eem.getQueueName()) || StringUtils.isEmpty(eem.getExchangeName())) {
			logger.warn(
					"The EventMessage's queueName and exchangeName is empty, this is not allowed, and processing message is canceled.");
			return;
		}
		EventProcessorWrap eepw = epwMap.get(eem.getQueueName() + "|" + eem.getExchangeName());
		if (eepw == null) {
			logger.warn("Receive an EopEventMessage,but not processor can do it");
			return;
		}
		try {
			eepw.process(eem.getEventData());
		} catch (IOException e) {
			logger.error("Event content can not be desrialized,check the provided CodecFactory", e);
			return;
		}
	}

	protected void add(String queueName, String exchangeName, EventProcesser processor, CodecFactory codecFactory) {
		if (StringUtils.isEmpty(queueName) || StringUtils.isEmpty(exchangeName) || processor == null
				|| codecFactory == null) {
			throw new RuntimeException(
					"queueName and exchangeName can not be empty,and processor or codecFactory can not be null. ");
		}
		EventProcessorWrap epw = new EventProcessorWrap(codecFactory, processor);
		EventProcessorWrap oldProcessorWrap = epwMap.putIfAbsent(queueName + "|" + exchangeName, epw);
		if (oldProcessorWrap != null) {
			logger.warn("The processor of this queue and exchange exists, and the new one can't be add");
		}
	}

	protected Set<String> getAllBinding() {
		Set<String> keySet = epwMap.keySet();
		return keySet;
	}

	protected static class EventProcessorWrap {
		private CodecFactory codecFactory;
		private EventProcesser eep;

		protected EventProcessorWrap(CodecFactory codecFactory, EventProcesser eep) {
			this.codecFactory = codecFactory;
			this.eep = eep;
		}

		public void process(byte[] eventData) throws IOException {
			Object obj = codecFactory.deSeriablize(eventData);
			eep.proccess(obj);
		}
	}
}
