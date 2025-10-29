package br.com.fiap.kafka_demo.controller;


import br.com.fiap.kafka_demo.service.KafkaMessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    private final KafkaMessageProducer producer;
    public KafkaController(KafkaMessageProducer producer){
        this.producer = producer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> sendMessage(@RequestParam String key, @RequestBody String message){
        try{
            producer.sendMessageWithKey(key, message);
            return ResponseEntity.ok("Mensagem com chave: '" + key + "' enviada para o Kafka: " + message);
        } catch (Exception e) {
         return ResponseEntity.internalServerError().body("Erro ao enviar mensagem: " + e.getMessage());
        }
    }
}