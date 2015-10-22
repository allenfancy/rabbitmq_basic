package com.allen.rabbitmq.rabbitmq;

public class SendRefuseException extends Exception {

	public SendRefuseException(){
		
	}
	public SendRefuseException(String msg){
		super(msg);
	}
	public SendRefuseException(Exception e){
		super(e);
	}
	public SendRefuseException(String msg,Exception e){
		super(msg,e);
	}
}
