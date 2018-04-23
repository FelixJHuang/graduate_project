package com.gp.spark.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class RealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"1", "2", "3", "4", "5"};
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	
	private Producer<Integer, String> producer;
	
	public RealTimeData() {
		provinceCityMap.put("1", new String[] {"1", "1.1"});
		provinceCityMap.put("1", new String[] {"2", "2.1"});
		provinceCityMap.put("1", new String[] {"3", "3.1"});
		provinceCityMap.put("1", new String[] {"4", "4.1"});
		provinceCityMap.put("1", new String[] {"5", "5.1"});
		
		producer = new Producer<Integer, String>(createProducerConfig());  
	}
	
	private ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.1.105:9092,192.168.1.106:9092,192.168.1.107:9092");  
		return new ProducerConfig(props);
	}
	
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);  
			producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));  
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		RealTimeData producer = new RealTimeData();
		producer.start();
	}
	
}
