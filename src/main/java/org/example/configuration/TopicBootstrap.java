package org.example.configuration;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicBootstrap {

	// Configure as needed
	private static final String BOOTSTRAP = "localhost:9092";
	private static final String TOPIC = "transactions";
	private static final int PARTITIONS = 12;   // choose based on expected throughput/parallelism
	private static final short RF = 1;
	private static final long RETENTION_MS = Duration.ofDays(7).toMillis();

	public static void initializeTopics() throws ExecutionException, InterruptedException {
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");

		try (AdminClient admin = AdminClient.create(props)) {
			Set<String> existing = admin.listTopics().names().get();
			if (!existing.contains(TOPIC)) {
				NewTopic lifecycle = new NewTopic(TOPIC, PARTITIONS, RF)
						.configs(Map.of(
								"retention.ms", String.valueOf(RETENTION_MS)
								// Optionally pin cleanup.policy ("delete" is default)
								// ,"cleanup.policy", "delete"
						));
				admin.createTopics(List.of(lifecycle)).all().get();
				System.out.println("Created topic: " + TOPIC);
			} else {
				System.out.println("Topic already exists: " + TOPIC);
			}

			// Optional: describe to confirm partitions
			admin.describeTopics(Set.of(TOPIC)).all().get()
					.forEach((name, info) ->
							System.out.printf("Topic %s -> partitions=%d%n", name, info.partitions().size()));
		}
	}
}
