package com.singularity.kafkaconsumer.processor;

import com.singularity.kafkaconsumer.model.User;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;


@Component
@EnableBinding(KafkaStreamsProcessor.class)
public class UserKafkaStreamProcessor {

    @StreamListener("input")
    @SendTo("output")
    public KStream<?, User> process(KStream<String, User> input) {
        System.out.println(input.getName())
        return input.map((key, value) -> new KeyValue<>(null, this.transformMessage(value)));
    }

    private User transformMessage(User user){

        if(user.getName().equals("Rafael")){
            user.setNickName("Veggie tiger");
        }else if(user.getName().equals("Nick")){
            user.setNickName("Daddy tiger");
        }else {
            user.setNickName("No nickname");
        }
        return user;
    }








}
