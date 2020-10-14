package com.messagebroker.producer;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import com.messagebroker.IMessageBroker;
import com.messagebroker.message.Message;

import lombok.Data;

@Data
public class Producer implements IProducer {

	private List<String> topics;

	private IMessageBroker broker;

	public Producer(List<String> topics, IMessageBroker broker) {
		this.broker = broker;
		this.topics = topics;

	}

	@Override
	public void send(Message message) {

		broker.send(message);

	}

	@Override
	public Message createMessage(String topic, String data) {

		return new Message(topic, data);
	}

	// for testing only
	public void sendBulkMessage(int numOfMessages) throws InterruptedException {

		ExecutorService executor = Executors.newFixedThreadPool(5);

		IntStream.range(0, numOfMessages).forEach(i -> {

			Runnable task = () -> {
				for (String topic : topics) {
					String data = "";

					switch (topic) {
					case "A":
						data = "This message is for topic A   # " + i;
						send(createMessage(topic, data));
						break;

					case "B":
						data = "This is some fake message for  topic B # " + i;
						send(createMessage(topic, data));
						break;

					case "C":
						data = "This  message  is for  topic C #" + i;
						send(createMessage(topic, data));
						break;
					case "D":
						data = "This fake message id for topic D #" + i;
						send(createMessage(topic, data));
						break;
					}

				}

			};
			executor.execute(task);
		});

		executor.shutdown();

	}

}
