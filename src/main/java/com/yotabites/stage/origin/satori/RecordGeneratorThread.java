package com.yotabites.stage.origin.satori;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import com.satori.rtm.model.*;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;



public class RecordGeneratorThread extends BasePushSource implements Runnable {
	
	private Queue<AnyJson> messageQueue = new LinkedList<AnyJson>();
	int threadId;
	
	RecordGeneratorThread(Queue<AnyJson> q)
	{
		this.messageQueue=q;
		
	}
	RecordGeneratorThread(int threadId)
	{
		this.threadId=threadId;
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		 if (messageQueue.size() != 0) {
	            for (AnyJson json : messageQueue) {
		BatchContext batchContext = getContext().startBatch();
		 Record record = ((com.streamsets.pipeline.api.Stage.Context) batchContext).createRecord("Thread #" + threadId);
		    Map<String, Field> map = new HashMap<>();
		    map.put("fieldName", Field.create(json.toString()));
		    record.set(Field.create(map));
		    batchContext.getBatchMaker().addRecord(record);
		    getContext().processBatch(batchContext);
	}
		  }
    }

	@Override
	public int getNumberOfThreads() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void produce(Map<String, String> arg0, int arg1) throws StageException {
		// TODO Auto-generated method stub
		
	}

}
