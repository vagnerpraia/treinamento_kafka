package treinamento.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer
 *
 */
public class Consumer {
    public static void main(String[] args){
        final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "teste_consumer";
        String autoOffsetReset = "earliest";
        
        // Propriedades do consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        // Criação do consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Inscrição de tópico(s)
        List<String> topics = Arrays.asList("teste");
        consumer.subscribe(topics);

        // Pesquisa novos dados
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info(record.value());
            }
        }
    }
}
