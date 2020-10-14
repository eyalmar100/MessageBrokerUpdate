package com.messagebroker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.messagebroker.consumer.IConsumerCallback;
import com.messagebroker.message.Message;
import com.messagebroker.queue.IQueue;
import com.messagebroker.queue.MyQueue;

public class MesssgeBroker implements IMessageBroker {

	Map<String, IQueue> queues;
	List<String> topics;

	public MesssgeBroker(List<String> topics) {
		this.topics = topics;
		queues = new HashMap<>();

		initQueues();

	}

	private void initQueues() {

		for (String topic : topics) {

			IQueue queue = new MyQueue(topic);
			queues.put(topic, queue);
		}
	}

	@Override
	public void register(String topic, IConsumerCallback consumer) {
		IQueue queue = queues.get(topic);
		consumer.setQueue(queue);
	}

	@Override
	public void send(Message message) {
		System.out.println("adding message to queue:"+message.getTopic());

		IQueue queue = queues.get(message.getTopic());
		if (queue != null) {

			queue.write(message);

		}

	}

}
