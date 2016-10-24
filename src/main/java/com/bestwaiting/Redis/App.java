package com.bestwaiting.Redis;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
         try {
        	 new Thread(new Producer()).start(); 
        	 Thread.sleep(15000);
        	 //启动一个线程者线程，模拟任务的处理  
	         new Thread(new Consumer()).start();  
	         //主线程休眠  
	         Thread.sleep(Long.MAX_VALUE);  
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }
}
