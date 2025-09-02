package org.example;

import org.example.configuration.TopicBootstrap;
import org.example.util.TransactionLogger;
import org.example.util.TransactionSimulator;

import java.util.concurrent.ExecutionException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
	public static void main(String[] args) throws Exception {
		TopicBootstrap.initializeTopics();

		TransactionLogger.consumeAndLogTransactions();
		TransactionSimulator.simulateBetsAndWins();
	}
}