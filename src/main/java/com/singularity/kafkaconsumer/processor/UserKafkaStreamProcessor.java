package com.singularity.kafkaconsumer.processor;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.singularity.kafkaconsumer.model.User;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.io.IOException;


@Component
@EnableBinding(KafkaStreamsProcessor.class)
public class UserKafkaStreamProcessor {


    @StreamListener("input")
    @SendTo("output")
    public KStream<?, String> process(KStream<String, String> input) {
        return input.map((key, value) -> new KeyValue<>(null, this.transformMessage(value)));
    }

    private String transformMessage(String  json){

        ObjectMapper mapper = new ObjectMapper();
        String transformedDate = null;

        try {
            User user = mapper.readValue(json, User.class);

            if(user.getName().equals("Rafael")){
                user.setNickName("Veggie tiger");
            }else if(user.getName().equals("Nick")){
                user.setNickName("Daddy tiger");
            }else {
                user.setNickName("No nickname");
            }
            transformedDate = mapper.writeValueAsString(user);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transformedDate;
    }

}
