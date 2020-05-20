package com.jsy.canaltest;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueTest {
	public static void main(String[] args) {
		Queue<String> q = new LinkedBlockingQueue<>();
		q.add("a");
		q.add("b");
		q.add("c");
		
		System.out.println(q.size());
		
		q.stream().forEach(s -> {
			System.out.println(s);
		});
		
		System.out.println(q.size());

//		for(int i=0; i<q.size(); i++) {
//			String s = q.poll();
//			System.out.println(s);
//		}
		
		while(q.peek()!=null) {
			System.out.println(q.poll());
		}
		
		System.out.println(q.size());
	}
}
