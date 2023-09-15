package com.example.producer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SpringBootApplication
public class  ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    /* Topic in apache kafka broker */
    static final String PAGE_VIEW_TOPIC = "pv_topic";
}

@Configuration
class RunnerApplication {

    void kafka(KafkaTemplate<Object, Object> template) {
        PageView pageView = random("kafka");
        template.send(ProducerApplication.PAGE_VIEW_TOPIC, pageView);
    }

    private PageView random(String source) {
        String[] names = "hai,vu,vy,hieu,lam,quan,nam".split(",");
        String[] pages = "home,news,about,career,login,register,transaction-history,setting".split(",");
        Random random = new Random();
        int randomNum = random.nextInt(pages.length);
        int randomName = random.nextInt(names.length);
        return new PageView(pages[randomNum], Math.random() < 0.5 ? 100 : 1000, names[randomName], source);
    }

    void stream(StreamBridge streamBridge) {
        streamBridge.send("pageViews-out-0", random("stream"));
    }


    void integration (MessageChannel channel) {
        Message<PageView> message = MessageBuilder
                .withPayload(random("integration"))
//				.copyHeadersIfAbsent(Map.of(KafkaHeaders.TOPIC, ProducerApplication.PAGE_VIEW_TOPIC)) // because we already configure the topic name in the random() method
                .build();

        channel.send(message);
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> runnerListener(
            StreamBridge streamBridge, KafkaTemplate<Object, Object> kafkaTemplate, MessageChannel channel) {
        return event -> {
//			kafka(kafkaTemplate);
//            integration(channel);
            for (int i = 0; i < 1000; i++) {
                stream(streamBridge);

            }
        };
    }
}


@Configuration
class IntegrationConfiguration {

    @Bean
    IntegrationFlow flow(MessageChannel channel, KafkaTemplate<Object, Object> kafkaTemplate)  {
        KafkaProducerMessageHandler<Object, Object> kafka = Kafka.outboundChannelAdapter(kafkaTemplate)
                .topic(ProducerApplication.PAGE_VIEW_TOPIC)
                .getObject();

        return IntegrationFlow
                .from(channel)
                .handle(new GenericHandler<PageView>() {
                    @Override
                    public Object handle(PageView payload, MessageHeaders headers) {
                        return null;
                    }
                })
                .get();

    }


    @Bean
    MessageChannel channel() {
        return MessageChannels.direct().getObject();
    }

}


@Configuration
class KafkaConfiguration {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer;


    @KafkaListener(topics = ProducerApplication.PAGE_VIEW_TOPIC, groupId = "pv_topic_group")
    public void onNewPageView(Message<PageView> pageView) {
        System.out.println("-------------------");
        System.out.println("new page view = " + pageView);
        pageView.getHeaders().forEach((s, o) -> System.out.println(s + "-" + o));
    }

    @Bean
    NewTopic pageViewTopic() {
        return new NewTopic(ProducerApplication.PAGE_VIEW_TOPIC, 1,(short) 1);
    }

    @Bean
    JsonMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }


    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return new KafkaTemplate<>(producerFactory, properties);
    }
}

record PageView (String page, long duration, String userId, String source) {}