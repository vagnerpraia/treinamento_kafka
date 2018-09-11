package io.dadospublicos.etl.siconv;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

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
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Criação do producer

        // Carregamento dos dados

    }
}
