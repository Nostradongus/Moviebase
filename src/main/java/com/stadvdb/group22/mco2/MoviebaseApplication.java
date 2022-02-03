package com.stadvdb.group22.mco2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.locks.ReentrantLock;

@SpringBootApplication
public class MoviebaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(MoviebaseApplication.class, args);
	}

}
