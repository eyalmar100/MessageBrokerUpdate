package com.messagebroker.message;

public class Message {

	private String topic;
	private String data;
	
	public Message(String topic,String data) {
		this.topic=topic;
		this.data=data;
	}
	
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}

}
