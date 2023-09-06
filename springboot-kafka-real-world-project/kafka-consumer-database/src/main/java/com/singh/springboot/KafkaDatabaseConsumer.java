package com.singh.springboot;

import com.singh.springboot.entity.WikimediaData;
import com.singh.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaDatabaseConsumer.class);
    @Autowired
    private WikimediaDataRepository wikimediaDataRepository;
    @KafkaListener(topics = "${spring.kafka.topic.name}",groupId = "${spring.kafka.consumer.group-id}")
    public  void consume( String eventMessge){
        LOGGER.info(String.format("Message recived -> %s",eventMessge));
        WikimediaData wikimediaData=new WikimediaData();
        wikimediaData.setWikiEventData(eventMessge);
        wikimediaDataRepository.save(wikimediaData);

    }
}
