package com.yotabites.stage.origin.satori;

import com.satori.rtm.*;
import com.satori.rtm.model.*;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.yotabites.stage.origin.satori.Groups;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

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
public class SatoriWSClient extends BaseSource {

    //private Queue<AnyJson> messageQueue = new LinkedList<AnyJson>();
	BlockingQueue<AnyJson> deque = new LinkedBlockingQueue<AnyJson>();
  
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
                    try {
						deque.put(json);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                }
            }
        };
        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
        client.start();
        return super.init();
    }


    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        int nextSourceOffset = 0;
        if (lastSourceOffset != null) {
            nextSourceOffset = Integer.parseInt(lastSourceOffset);
        }
              while(!deque.isEmpty())
              {
                Record record = getContext().createRecord("some-id::" + nextSourceOffset);
                Map<String, Field> map = new HashMap<>();
                try {
					map.put("fieldName", Field.create(deque.take().toString()));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                record.set(Field.create(map));
                batchMaker.addRecord(record);
                ++nextSourceOffset;
              }
       
        return String.valueOf(nextSourceOffset);
    }

    @Override
    public void destroy() {
        client.stop();
        super.destroy();
    }

}
