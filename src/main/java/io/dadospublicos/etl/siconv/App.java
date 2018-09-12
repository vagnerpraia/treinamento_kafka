package io.dadospublicos.etl.siconv;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * ETL Siconv
 *
 */
public class App {
    public static void main(String[] args){
        System.out.println("Início ETL Siconv");

        String bootstrapServers = "127.0.0.1:9092";

        // Propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criação do producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Criação do registros do producer
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("siconv", "ola mundo");

        // Carregamento dos dados
        producer.send(record);

        // Forçar o carregamento dos dados no cunsumer
        producer.flush();
        producer.close();

        System.out.println("Fim ETL Siconv");
    }
}
