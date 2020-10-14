package com.messagebroker.producer;

import com.messagebroker.message.Message;

public interface IProducer {
	
	Message createMessage(String topic,String data);
	void send(Message message);

}
