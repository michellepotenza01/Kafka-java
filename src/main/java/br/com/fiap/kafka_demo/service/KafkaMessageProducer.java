package br.com.fiap.kafka_demo.service;


import br.com.fiap.kafka_demo.config.KafkaTopicConfig;
import jakarta.websocket.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final KafkaTemplate<String, String> KafkatemKafkaTemplate;
    private final String topicName;

    public KafkaMessageProducer(kafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, String> kafkatemKafkaTemplate, KafkaTemplate<String, String> kafkatemKafkaTemplate1, KafkaTemplate<String, String> kafkatemKafkaTemplate2, KafkaTemplate<String, String> kafkatemKafkaTemplate3),
                            @Value("${app.kafka.topic.meu-topico}") String getTopicName) {
    this.kafkaTemplate = kafkaTemplate;
    this.topicName = topicName;
    }

    public void sendMessage(String message) {
        log.info("Enviando mensagem: '{}' para o topico '{}'", message, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((SendResult<String, String> result, Throwable ex) ->) {
            if (ex == null) {
                log.info("Mensagem enviad com sucesso para o topico '{}' pertição '{}' com offset '{}'",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().parition(),
                        result.getRecordMetadata().offset());
            }else {
                log.error(("Falha ao enviar mensagem para o topico '{}' : {}", topicName, ex.getMessage());
            }
        };
        }

    public void sendMessage(String key, String message) {
        log.info("Enviando mensagem: '{}' com chave '{}' para o topico '{}'", message, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName,key, message);
        future.whenComplete((SendResult<String, String> result, Throwable ex) ->) {
            if (ex == null) {
                log.info("Mensagem com chave '{}' enviad com sucesso para o topico '{}' pertição '{}' com offset '{}'",
                        result.getProducerRecord().key(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().parition(),
                        result.getRecordMetadata().offset());
            }else {
                log.error(("Falha ao enviar mensagem com chave '{}' para o topico '{}' : {}", topicName, ex.getMessage());
            }
        };
    }
}




