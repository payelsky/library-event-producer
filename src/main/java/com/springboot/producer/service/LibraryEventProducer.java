package com.springboot.producer.service;



import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springboot.producer.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventProducer {
	
@Autowired
private KafkaTemplate<Integer,String> kafkaTemplate;

@Autowired
ObjectMapper objectMapper;


public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
Integer key=libraryEvent.getLibraryEventId();
String value=objectMapper.writeValueAsString(libraryEvent);
ListenableFuture<SendResult<Integer,String>> listenableFuture=kafkaTemplate.sendDefault(key, value);
listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer,String>>(){

	@Override
	public void onSuccess(SendResult<Integer, String> result) {
		handleSuccess(key,value,result);
		
	}
	@Override
	public void onFailure(Throwable ex) {
		
		handleFailure(key,value,ex);
	}

});
}

private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
	log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}",key,value,result.getRecordMetadata().partition());
	
}

public SendResult<Integer,String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
	Integer key = libraryEvent.getLibraryEventId();
	String value = objectMapper.writeValueAsString(libraryEvent);
	SendResult<Integer,String>	sendResult=null;
	try {
	sendResult=kafkaTemplate.sendDefault(key, value).get(1,TimeUnit.SECONDS);
	} catch (InterruptedException | ExecutionException e) {
		 log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
         throw e;
	} catch (Exception e) {
        log.error("Exception Sending the Message and the exception is {}", e.getMessage());
        throw e;
    }
	return sendResult;
}

private void handleFailure(Integer key, String value, Throwable ex) {
    log.error("Error Sending the Message and the exception is {}", ex.getMessage());
    try {
        throw ex;
    } catch (Throwable throwable) {
        log.error("Error in OnFailure: {}", throwable.getMessage());
    }
}


}
