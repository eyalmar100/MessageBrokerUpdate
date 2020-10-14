package com.messagebroker.main;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.messagebroker.IMessageBroker;
import com.messagebroker.MesssgeBroker;
import com.messagebroker.consumer.Consumer;
import com.messagebroker.consumer.Consumers;
import com.messagebroker.producer.Producer;

public class Main {

	
	// helper for desirialize json array
	class Topics {
		List<String> topics;
	}

	public static void main(String[] args) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		
		int numOfMessages=50000;
		
		
		if(args.length==1) {
			numOfMessages=Integer.parseInt(args[0]);
		}
		
		Gson gson = new Gson();
        
		Consumers consumers = gson.fromJson(new FileReader("src/resources/consumers.json"), Consumers.class);
		Topics topics = gson.fromJson(new FileReader("src/resources/topics.json"), Topics.class);

		IMessageBroker broker = new MesssgeBroker(topics.topics);
		
		
		createConsumersFromJson(consumers, broker);
		
		Producer producer=new Producer(topics.topics,broker);
		try {
			producer.sendBulkMessage(numOfMessages);
			
			 
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		 
		
}

	private static void createConsumersFromJson(Consumers consumers, IMessageBroker broker) {
		for (Consumer consumer : consumers.getConsumers()) {

			 
			
			consumer = new Consumer (consumer.getName(),consumer.getTopics(),broker);
					 
	 
		}
	}
}