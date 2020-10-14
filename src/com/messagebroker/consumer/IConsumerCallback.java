package com.messagebroker.consumer;

import java.util.List;

import com.messagebroker.queue.IQueue;

public interface IConsumerCallback {
	
	 
	void setQueue(IQueue queue);
	//List<IQueue> getQueues();
}
