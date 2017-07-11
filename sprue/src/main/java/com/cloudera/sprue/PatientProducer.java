package com.cloudera.sprue;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PatientProducer {

	public static void main(String[] args) throws Exception{

		// Check arguments length value

		//Assign topicName to string variable
		String topicName = "Hello-Kafka";

		// create instance for properties to access producer configs   
		Properties props = new Properties();

		//Assign localhost id
		props.put("bootstrap.servers", "34.211.90.97:9092");

		//Set acknowledgements for producer requests.      
		props.put("acks", "all");

		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		//Specify buffer size in config
		props.put("batch.size", 16384);

		//Reduce the no of requests less than 0   
		props.put("linger.ms", 1);

		//The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", 
				"com.cloudera.sprue.PatientSerializer");

		Thread.currentThread().setContextClassLoader(null);
		Producer<String, Patient> producer = new KafkaProducer<String, Patient>(props);

		Random rand = new Random();
		String locations[] = {"TX", "CA", "OK", "NY", "FL", "GA", "IL", "NE", "MN", "SC", "NM", "VA", "LA", "KS", "MS"};

		for (int i = 1; i < 250 ; i++) {

			Patient patient = new Patient("pikachu_" + i + "_machop" + i, System.currentTimeMillis(), locations[rand.nextInt(locations.length)]);

			if (i % 5 == 0) {
				patient.setTemperature(90 + rand.nextInt(14) * rand.nextFloat());
				patient.setHeartRate(20 + rand.nextInt(90));
				patient.setHypotension(rand.nextInt(2));
				patient.setInfectionFlag("Y");
				patient.setOrganFailCount(rand.nextInt(3));
				patient.setWbc(8000 + rand.nextInt(11000));
				patient.setRespiratoryRate(30 + rand.nextInt(20));
				patient.setSbp(rand.nextInt(100));
			} else if (i % 5 == 1) {
				patient.setTemperature(90 + rand.nextInt(14) * rand.nextFloat());
				patient.setHeartRate(20 + rand.nextInt(90));
				patient.setHypotension(rand.nextInt(2));
				patient.setInfectionFlag("N");
				patient.setOrganFailCount(rand.nextInt(4));
				patient.setWbc(3000 + rand.nextInt(11000));
				patient.setRespiratoryRate(10 + rand.nextInt(20));
				patient.setSbp(rand.nextInt(100));
			} else if (i % 5 == 2) {
				patient.setTemperature(90 + rand.nextInt(14) * rand.nextFloat());
				patient.setHeartRate(20 + rand.nextInt(90));
				patient.setHypotension(rand.nextInt(2));
				patient.setInfectionFlag("Y");
				patient.setOrganFailCount(rand.nextInt(3));
				patient.setWbc(3000 + rand.nextInt(11000));
				patient.setRespiratoryRate(10 + rand.nextInt(20));
				patient.setSbp(rand.nextInt(100));
			} else if (i % 5 == 3) {
				patient.setTemperature(90 + rand.nextInt(14) * rand.nextFloat());
				patient.setHeartRate(20 + rand.nextInt(90));
				patient.setHypotension(rand.nextInt(2));
				patient.setInfectionFlag("N");
				patient.setOrganFailCount(rand.nextInt(5));
				patient.setWbc(3000 + rand.nextInt(11000));
				patient.setRespiratoryRate(10 + rand.nextInt(20));
				patient.setSbp(rand.nextInt(100));
			} else if (i % 5 == 4) {
				patient.setTemperature(90 + rand.nextInt(14) * rand.nextFloat());
				patient.setHeartRate(20 + rand.nextInt(90));
				patient.setHypotension(rand.nextInt(2));
				patient.setInfectionFlag("Y");
				patient.setOrganFailCount(rand.nextInt(6));
				patient.setWbc(3000 + rand.nextInt(11000));
				patient.setRespiratoryRate(10 + rand.nextInt(20));
				patient.setSbp(rand.nextInt(100));
			}

			System.out.println("------------------- Patient ------ " + patient.toString());
			producer.send(new ProducerRecord<String, Patient>(topicName, 
					patient.getPatientId(), patient));
			Thread.sleep(250);
		}

		System.out.println("Message sent successfully");
		producer.close();
	}

}
