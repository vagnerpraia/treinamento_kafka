package io.dadospublicos.etl.siconv;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * ETL Siconv
 *
 */
public class App {
    public static void main(String[] args){
        System.out.println("ETL Siconv");

        String bootstrapServers = "192.168.0.101:9092";

        // Propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criação do producer

        // Carregamento dos dados

    }
}
