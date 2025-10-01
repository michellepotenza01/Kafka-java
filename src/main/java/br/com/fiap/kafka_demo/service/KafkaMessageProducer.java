package br.com.fiap.kafka_demo.service;

import br.com.fiap.kafka_demo.config.KafkaTopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${app.kafka.topic.meu-topico}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessage(String message) {
        log.info("Enviando mensagem: '{}' para o topico '{}'", message, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((SendResult<String, String> result, Throwable ex) -> {
            if (ex == null) {
                log.info("Mensagem enviada com sucesso para o topico '{}' partição '{}' com offset '{}'",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Falha ao enviar mensagem para o topico '{}' : {}", topicName, ex.getMessage());
            }
        });
    }

    public void sendMessage(String key, String message) {
        log.info("Enviando mensagem: '{}' com chave '{}' para o topico '{}'", message, key, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);
        future.whenComplete((SendResult<String, String> result, Throwable ex) -> {
            if (ex == null) {
                log.info("Mensagem com chave '{}' enviada com sucesso para o topico '{}' partição '{}' com offset '{}'",
                        result.getProducerRecord().key(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Falha ao enviar mensagem com chave '{}' para o topico '{}' : {}", key, topicName, ex.getMessage());
            }
        });
    }
}
