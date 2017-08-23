package com.yotabites.stage.origin.satori;

import com.satori.rtm.*;
import com.satori.rtm.model.*;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.yotabites.stage.origin.satori.Groups;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by yesh on 8/22/17.
 */

@StageDef(
        version = 0,
        label = "SatoriWS",
        description = "",
        icon = "satori-32x32.png",
        execution = ExecutionMode.STANDALONE,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class SatoriWSClient extends BasePushSource   {

    private Queue<AnyJson> messageQueue = new LinkedList<AnyJson>();
    private RtmClient client;
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "wss://open-data.api.satori.com",
            label = "EndPoint",
            description = "",
            displayPosition = 10,
            group = "Satori"
    )
    public String endpoint;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "AppKey",
            description = "AppKey",
            displayPosition = 20,
            group = "Satori"
    )
    public String appkey;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "",
            label = "Channel",
            description = " ",
            displayPosition = 20,
            group = "Satori"
    )
    public String channel;
    @ConfigDef(
    	     required = false,
    	     type = ConfigDef.Type.NUMBER,
    	     defaultValue = "1",
    	     label = "Thread Count",
    	     displayPosition = 10,
    	     group = "Satori"
    	)
    	public int threadCount;
    @Override
    protected List<ConfigIssue> init() {
        client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {

                })
                .build();
        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                    messageQueue.add(json);
                    new RecordGeneratorThread(messageQueue);
                }
            }
        };
        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
        client.start();
       
        return super.init();
    }


    @Override
    public void destroy() {
        client.stop();
        super.destroy();
    }


	@Override
	public int getNumberOfThreads() {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void produce(Map<String, String> arg0, int arg1) throws StageException {
		
		ExecutorService executor = Executors.newFixedThreadPool(threadCount); 
		List<Future<Runnable>> futures = new ArrayList<>(threadCount);
		        
		// Start the threads
		for(int i = 0; i < threadCount; i++) {
		      Future future = executor.submit(new RecordGeneratorThread(i));
		      futures.add(future);
		}
		        
		// Wait for execution end
		for(Future<Runnable> f : futures) {
		      try {
		          f.get();
		      } catch (InterruptedException|ExecutionException e) {
		         
		      }
		      finally {
		    	  executor.shutdownNow();
			}
		      
		}
		
		// TODO Auto-generated method stub
		
	}

}
