package com.messagebroker;

import com.messagebroker.consumer.IConsumerCallback;
import com.messagebroker.message.Message;

public interface IMessageBroker {
	
	void register(String topic, IConsumerCallback consumer);
	void send(Message message);
 	 

}
