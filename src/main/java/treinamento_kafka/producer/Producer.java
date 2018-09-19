package treinamento_kafka.producer;

import java.lang.management.ManagementFactory;
import java.lang.Runnable;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    final Logger logger = LoggerFactory.getLogger(Producer.class.getName());

    public static void main(String[] args){
        new Producer().execute();
    }

    public void execute(){
        Integer threadCount = ManagementFactory.getThreadMXBean().getThreadCount();

        CountDownLatch latch = new CountDownLatch(threadCount);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "teste_consumer";
        String autoOffsetReset = "earliest";
        String topic = "teste";

        final Runnable producerRunnable = new ProducerRunnable(latch, bootstrapServers, groupId, autoOffsetReset, topic);

        Thread producer = new Thread(producerRunnable);
        producer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Aplicação encerrada.");
            ((ProducerRunnable) producerRunnable).shutdown();
        }));

        try{
            latch.await();
        }catch(InterruptedException e){
            logger.error("Aplicação foi enterrompida.", e);
        }finally{
            logger.info("Aplicação encerrada.");
        }
    }
}
