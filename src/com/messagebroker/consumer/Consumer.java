package com.messagebroker.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.messagebroker.IMessageBroker;
import com.messagebroker.message.Message;
import com.messagebroker.queue.IQueue;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Consumer implements IConsumerCallback {

	private String name;
	private Map<String, IQueue> queues;
	private Thread thread;
	private boolean isRunning;
	private List<String> topics;
	private IMessageBroker broker;

	public Consumer(String name, Map<String, IQueue> queues, Thread thread, final boolean isRunning,
			List<String> topics, IMessageBroker broker) {
		this.name = name;

		this.queues = new HashMap<>();

		for (String topic : topics) {
			broker.register(topic, this);
		}

		readFromQueue();

	}

	public Consumer(String name, List<String> topics, IMessageBroker broker) {
		this.name = name;

		this.queues = new HashMap<>();

		for (String topic : topics) {
			broker.register(topic, this);
		}

	 
		readWithDelay();
	}

	private void readWithDelay() {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);
		executor.scheduleAtFixedRate( () -> {

			Collection<IQueue> s = queues.values();

			System.out.println("   testing ");

			for (IQueue queue : s) {

				Message m = queue.read();
				System.out.println("in the looping");
				 System.out.printf("reading from Consumer: %s ,topis :%s,data :%s \n",
				 this.name,
				 queue.getTopicName(), m.getData());

				processMessage(m);
			}

		}, 0, 1, TimeUnit.MILLISECONDS);
	}
	
	void read() {
		
	}

	private void readWithRange() {

		ExecutorService executor = Executors.newFixedThreadPool(5);
	 
		IntStream.range(0, 1).forEach(i -> {

			Runnable task = () -> {

				Collection<IQueue> s = queues.values();

				System.out.println("   testing ");

				for (IQueue queue : s) {

					Message m = queue.read();
					System.out.println("in the looping");
					 System.out.printf("reading from Consumer: %s ,topis :%s,data :%s \n",
					 this.name,
					 queue.getTopicName(), m.getData());

					processMessage(m);
				}

			};
			executor.execute(task);
		});
		 
		 
	}

	private void readFromQueueWithExecutorService() {

		ExecutorService executor = Executors.newFixedThreadPool(5);

		while (true) {
			for (int i = 0; i < 50; ++i) {

				Collection<IQueue> s = queues.values();

				Runnable task = () -> {
					System.out.println("   testing ");

					for (IQueue queue : s) {

						Message m = queue.read();
						System.out.println("in the looping");
					    System.out.printf("reading from Consumer: %s ,topis :%s,data :%s \n",
						this.name,
						 queue.getTopicName(), m.getData());

						// processMessage(m);
					}

				};
				executor.execute(task);
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public Consumer(String name, List<String> topics) {
		this.name = name;
		this.topics = topics;
		queues = new HashMap<>();

	}

	private void readFromQueue() {
		Runnable task = () -> {
			// System.out.println("Task is running");
			Collection<IQueue> s = queues.values();
			while (isRunning) {

				for (IQueue queue : s) {

					// System.out.println(" reading from Consumer " + this.name + " topic:" +
					// queue.getTopicName());
					Message m = queue.read();

					System.out.printf("reading from Consumer: %s ,topis :%s,data :%s \n", this.name,
							queue.getTopicName(), m.getData());

					processMessage(m);
				}
			}
		};

		thread = new Thread(task);
		thread.start();
		isRunning = true;
	}

	private void processMessage(Message m) {
		try {

			Thread.sleep(10);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}

	@Override
	public void setQueue(IQueue queue) {
		queues.put(queue.getTopicName(), queue);

	}

}
