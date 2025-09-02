package org.example.util;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionLogger {
	private static final String BOOTSTRAP = "localhost:9092";
	private static final String TRANSACTIONS_TOPIC = "transactions";
	private static final String GROUP_ID = "transactions-logger-1";

	public static void consumeAndLogTransactions() {
		final KafkaConsumer<String, String> consumer;
		final Thread worker;
		final AtomicBoolean running = new AtomicBoolean(false);

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// On first run (no committed offsets), read from beginning:
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// Simple logging use-case; auto-commit is fine:
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		// Optional: identify this client instance for metrics/logs
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "transaction-logger-" + System.currentTimeMillis());

		consumer = new KafkaConsumer<>(props);

		worker = new Thread(() -> {
			try {
				consumer.subscribe(List.of(TRANSACTIONS_TOPIC));
				while (running.get()) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
					records.forEach(r -> System.out.printf(
							"CONSUMER: topic=%s p=%d off=%d ts=%d key=%s value=%s%n",
							r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), r.value()
					));
				}
			} catch (WakeupException ignore) {
				// Expected on stop()
			} finally {
				try {
					consumer.close();
				} catch (Exception e) {
					System.err.println("Consumer close error: " + e.getMessage());
				}
			}
		}, "transactions-logger");

		if (running.compareAndSet(false, true)) {
			worker.start();
		}
	}
}
