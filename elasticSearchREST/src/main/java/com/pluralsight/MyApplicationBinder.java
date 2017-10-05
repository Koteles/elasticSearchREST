package com.pluralsight;

import org.elasticsearch.client.Client;

import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.pluralsight.repository.StudentRepository;
import com.pluralsight.repository.StudentRepositoryStub;

public class MyApplicationBinder extends AbstractBinder {
	
    @Override
    protected void configure() {
        bind(StudentRepositoryStub.class).to(StudentRepository.class);
        bind(Client.class).to(Client.class);
    }
}
