package com.allen.rabbitmq.rabbitmq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;


public class HessionCodecFactory implements CodecFactory {

	private  Logger logger = LoggerFactory.getLogger(HessionCodecFactory.class);

	public byte[] serialize(Object obj) throws IOException {
		// TODO Auto-generated method stub
		ByteArrayOutputStream baos = null;
		HessianOutput output = null;
		try {
			baos = new ByteArrayOutputStream(1024);
			output = new HessianOutput(baos);
			output.startCall();
			output.writeObject(obj);
			output.completeCall();
		} catch (final IOException ex) {
			throw ex;
		} finally {
			if (output != null) {
				try {
					baos.close();
				} catch (final IOException ex) {
					logger.error("关闭流失败！", ex);
				}
			}
		}
		return baos != null ? baos.toByteArray() : null;
	}

	public Object deSeriablize(byte[] in) throws IOException {
		// TODO Auto-generated method stub
		Object obj = null;
		ByteArrayInputStream bais = null;
		HessianInput input = null;
		try {
			bais = new ByteArrayInputStream(in);
			input = new HessianInput(bais);
			input.startReply();
			obj = input.readObject();
			input.completeReply();
		} catch (final IOException ex) {
			throw ex;
		} catch (final Throwable e) {
			this.logger.error("Failed to decode object.", e);
		} finally {
			if (input != null) {
				try {
					bais.close();
				} catch (final IOException ex) {
					this.logger.error("Failed to close stream.", ex);
				}
			}
		}
		return obj;
	}
}
