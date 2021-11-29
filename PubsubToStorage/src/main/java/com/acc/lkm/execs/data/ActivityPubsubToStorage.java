
package com.acc.lkm.execs.data;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.acc.lkm.execs.data.ActivityPubsubToStorage.PubSubMessage;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class ActivityPubsubToStorage implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ActivityPubsubToStorage.class.getName());

  @Override
  public void accept(PubSubMessage message, Context context) {
    String data = message.data != null
      ? new String(Base64.getDecoder().decode(message.data))
      : "Hello, World";
    logger.info(data);
     // The ID of your GCP project
     String projectId = "playground-s-11-cf3db563";

    // The ID of your GCS bucket
    String bucketName = "activity-event-store";

    // The ID of your GCS object
    String objectName = generateFileName();

    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    logger.info("Writing on file " + data);
    logger.info("message.toString"  + message.toString());
    storage.create(blobInfo, data.getBytes());
  }

  public static class PubSubMessage {
    String data;
    Map<String, String> attributes;
    String messageId;
    String publishTime;
  }
  private String generateFileName() {
	
	//Generating the File name for storing the object in the cloud storage

	  SimpleDateFormat formatter = new SimpleDateFormat("dd_MM_yy");  
	    Date date = new Date();  
	    UUID uuid=UUID.randomUUID();  
	  return "activity-" + formatter.format(date) + "-" + uuid; 
	}
}

