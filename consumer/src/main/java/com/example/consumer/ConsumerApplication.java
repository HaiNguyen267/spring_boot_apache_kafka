package com.example.consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Bean
	Function<KStream<String, PageView>, KStream<String, Long>> counter() {
		return pvs -> pvs
				.filter((s, pageView) -> pageView.duration() > 100)
				.map((s, pageView) -> new KeyValue<>(pageView.page(), 0L))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
				.count(Materialized.as("pcmy"))
				.toStream();
	}


	@Bean
	Consumer<KTable <String, Long>> logger() {
		return counts -> counts
				.toStream()
				.foreach((s, aLong) -> System.out.println("page: " + s + ", count = " + aLong));
	}

}


record PageView(String page, long duration, String userId, String source) {}