package io.dadospublicos.etl.siconv;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL Siconv
 *
 */
public class App {
    public static void main(String[] args){
        System.out.println("Início ETL Siconv");

        final Logger logger = LoggerFactory.getLogger(App.class);

        String bootstrapServers = "127.0.0.1:9092";

        // Propriedades do producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criação do producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        List<Integer> itens = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        for(Integer item : itens){
            // Criação do registros do producer
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("siconv", "teste_" + Integer.toString(item));

            // Carregamento dos dados
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e){
                    if(e == null){
                        logger.info("Dados carregados:" + 
                            "\nTopic: " + metadata.topic() + 
                            "\nPartition: " + metadata.partition() + 
                            "\nOffset: " + metadata.offset() + 
                            "\nTimestamp: " + metadata.timestamp()
                        );
                    }else{
                        logger.error("Ocorreu um erro.", e);
                    }
                }
            });
        }

        // Forçar o carregamento dos dados no cunsumer
        producer.flush();
        producer.close();

        System.out.println("Fim ETL Siconv");
    }
}
