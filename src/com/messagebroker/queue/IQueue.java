package com.messagebroker.queue;

import com.messagebroker.message.Message;

public interface IQueue {
	
	String getTopicName();

	void write(Message message);

	Message read();

}
