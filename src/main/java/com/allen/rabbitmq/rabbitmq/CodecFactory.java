package com.allen.rabbitmq.rabbitmq;

import java.io.IOException;

public interface CodecFactory {

	byte[] serialize(Object obj) throws IOException;
	
	Object deSeriablize(byte[] in) throws IOException;
}
