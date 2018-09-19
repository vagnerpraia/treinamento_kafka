package treinamento_kafka.producer;

import java.lang.Runnable;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerRunnable implements Runnable {
    final Logger logger = LoggerFactory.getLogger(ProducerRunnable.class.getName());

    private CountDownLatch latch;
    private KafkaProducer<String, String> producer;
    private String topic;

    public ProducerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String autoOffsetReset, String topic){
        this.latch = latch;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<String, String>(properties);

        this.topic = topic;
    }

    @Override
    public void run(){
        try{
            List<Integer> itens = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            for(Integer item : itens){
                String key = "id_" + Integer.toString(item);
                String value = "teste_" + Integer.toString(item);

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

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
        }catch(WakeupException e){
            logger.info("Consumer desligado.");
            shutdown();
        }finally{
            producer.close();
            latch.countDown();
        }
    }

    public void shutdown(){
        producer.abortTransaction();
    }
}
