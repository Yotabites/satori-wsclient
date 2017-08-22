/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.stage.origin.sample;

import java.util.HashMap;
import java.util.Map;

import com.satori.rtm.SubscriptionListener;
import com.satori.rtm.model.AnyJson;
import com.satori.rtm.model.SubscriptionData;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "Sample Origin",
    description = "",
    icon = "default.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class SampleDSource extends SampleSource implements SubscriptionListener{

	@ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    defaultValue = "wss://open-data.api.satori.com",
		    label = "EndPointKey",
		    description = "",
		    displayPosition = 10,
		    group = "SAMPLE"
		)
		public String endpoint;

		/** {@inheritDoc} */
	    @Override
		public String getEndPoint() {
		  return endpoint;
		}

		@ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    defaultValue = "",
		    label = "Appkey",
		    description = "dEe74d6a44CFAD4AaFCBCc903Fe95fAb",
		    displayPosition = 20,
		    group = "SAMPLE"
		)
		public String appkey;

		/** {@inheritDoc} */
		@Override
		public String getAppKey() {
		  return appkey;
		}

		@ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    defaultValue = "cryptocurrency-market-data",
		    label = "Channel",
		    description = " ",
		    displayPosition = 20,
		    group = "SAMPLE"
		)
		public String channel ;

		/** {@inheritDoc} */
		 @Override
		public String getChannel() {
		  return channel;

}
		 @Override
			public void onSubscriptionData(SubscriptionData data) {

				for (AnyJson json : data.getMessages()) {

					Record record = getContext().createRecord("some-id::" + e.nextSourceOffset);
					Map<String, Field> map = new HashMap<>();
					map.put("fieldName", Field.create(json.toString()));
					record.set(Field.create(map));
					batchMaker.addRecord(record);
					++e.nextSourceOffset;

				}

			}


}
