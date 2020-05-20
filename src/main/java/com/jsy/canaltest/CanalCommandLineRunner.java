package com.jsy.canaltest;

import javax.annotation.Resource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.jsy.canaltest.canal.client.CanalClient;

@Component
public class CanalCommandLineRunner implements CommandLineRunner{
	@Resource
	CanalClient canalClient;
	
	@Override
	public void run(String... args) throws Exception {
		canalClient.process();
	}

}
