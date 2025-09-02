package org.example.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.BetEvent;
import org.example.model.WinEvent;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

public class TransactionSimulator {

	private static final String BOOTSTRAP = "localhost:9092";
	private static final String TRANSACTIONS_TOPIC = "transactions";
	private static final ObjectMapper MAPPER = new ObjectMapper();

	// Brands to simulate (uniform traffic assumption)
	private static final String[] BRANDS = new String[] {
			"brand-a", "brand-b", "brand-c", "brand-d", "brand-e", "brand-f"
	};

	public static void simulateBetsAndWins() throws Exception {
		// Build one shared producer instance for both topics
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {

			ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

			// Schedule a bet every 5 seconds
			final long betPeriodMs = Duration.ofSeconds(5).toMillis();
			scheduler.scheduleAtFixedRate(() -> {
				try {
					// 1) create bet
					EventBundle bundle = randomBet();
					// 2) send bet (key = brandId)
					var betJson = MAPPER.writeValueAsString(bundle.bet);
					producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC, bundle.bet.sessionId, betJson), (meta, ex) -> {
						if (ex != null) {
							System.err.println("Failed to send bet: " + ex.getMessage());
						} else {
							System.out.printf("PRODUCER: BET  -> %s-%d@%d key=%s betId=%s amount=%.2f%n",
									meta.topic(), meta.partition(), meta.offset(),
									bundle.bet.brandId, bundle.bet.betId, bundle.bet.amount);
						}
					});

					// 3) schedule the matching win 5 seconds later
					scheduler.schedule(() -> {
						try {
							var winJson = MAPPER.writeValueAsString(bundle.win);
							producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC, bundle.win.sessionId, winJson), (meta, ex) -> {
								if (ex != null) {
									System.err.println("Failed to send win: " + ex.getMessage());
								} else {
									System.out.printf("PRODUCER: WIN  -> %s-%d@%d key=%s betId=%s win=%.2f%n",
											meta.topic(), meta.partition(), meta.offset(),
											bundle.win.brandId, bundle.win.betId, bundle.win.winAmount);
								}
							});
						} catch (Exception e) {
							System.err.println("Failed to serialize/send win: " + e.getMessage());
						}
					}, 5, TimeUnit.SECONDS);

				} catch (Exception e) {
					System.err.println("Failed to produce bet: " + e.getMessage());
				}
			}, 0, betPeriodMs, TimeUnit.MILLISECONDS);

			// Keep the app alive
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				System.out.println("Shutting down simulator...");
				scheduler.shutdown();
				try {
					scheduler.awaitTermination(5, TimeUnit.SECONDS);
				} catch (InterruptedException ignored) {}
			}));

			// Block indefinitely
			Thread.currentThread().join();
		}
	}

	private static Properties producerProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
		return props;
	}

	private record EventBundle(BetEvent bet, WinEvent win) {}

	private static final Random RND = new Random();

	private static EventBundle randomBet() {
		String brandId = BRANDS[RND.nextInt(BRANDS.length)];
		String gameId = "GAME_KAFKA_TEST";
		String currency = "USD";
		String playerId = "player-" + (1000 + RND.nextInt(9000));
		String betId = "bet-" + UUID.randomUUID();
		String sessionId = UUID.randomUUID().toString();

		double amount = round2(1 + (RND.nextDouble() * 99)); // 1.00 .. 100.00
		double payoutMultiplier = 0.5 + (RND.nextDouble() * 3.0); // 0.5x .. 3.5x
		double winAmount = round2(amount * payoutMultiplier);

		BetEvent bet = new BetEvent();
		bet.eventId = UUID.randomUUID().toString();
		bet.timestamp = System.currentTimeMillis();
		bet.betId = betId;
		bet.playerId = playerId;
		bet.sessionId =  sessionId;
		bet.gameId = gameId;
		bet.brandId = brandId;
		bet.currency = currency;
		bet.amount = amount;

		WinEvent win = new WinEvent();
		win.eventId = UUID.randomUUID().toString();
		win.timestamp = System.currentTimeMillis() + 5000; // simulate +5s later
		win.betId = betId;
		win.sessionId = sessionId;
		win.playerId = playerId;
		win.gameId = gameId;
		win.brandId = brandId;
		win.currency = currency;
		win.winAmount = winAmount;

		return new EventBundle(bet, win);
	}

	private static double round2(double v) {
		return Math.round(v * 100.0) / 100.0;
	}
}
