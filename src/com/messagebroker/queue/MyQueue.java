package com.messagebroker.queue;

import java.util.concurrent.LinkedBlockingQueue;

import com.messagebroker.message.Message;

import lombok.Data;

@Data
public class MyQueue implements IQueue {

	private String topicName;
	// Thread safe - no worry about pop/push synchonization 
	private LinkedBlockingQueue<Message> queue;

	private String topic;

	public MyQueue(String topic) {
		topicName = topic;
		queue = new LinkedBlockingQueue<>();
		this.topic = topic;

	}

	@Override
	public void write(Message message) {

		try {
			queue.put(message);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public Message read() {

		Message m = null;

		try {
			m = queue.take();
		}
		catch (InterruptedException e) {

			e.printStackTrace();
		}

		return m;

	}

	@Override
	public String getTopicName() {
		 
		return topicName;
	}

}
