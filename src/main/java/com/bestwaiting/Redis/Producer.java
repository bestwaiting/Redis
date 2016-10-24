package com.bestwaiting.Redis;

import java.util.Random;
import java.util.UUID;

public class Producer implements Runnable {

	public void run() {
		// TODO Auto-generated method stub
		Random random = new Random();  
        while(true){  
            try{  
                Thread.sleep(random.nextInt(600) + 600);  
                // 模拟生成一个任务  
                UUID taskid = UUID.randomUUID();  
                //将任务插入任务队列：task-queue  
                RedisUtils.lpush("task-queue", taskid.toString());  
                System.out.println("插入了一个新的任务： " + taskid);  
            }catch(Exception e){  
                e.printStackTrace();  
            }  
        }  
	}

}
