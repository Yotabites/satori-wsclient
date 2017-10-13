# satori-wsclient
[Satori](https://www.satori.com/) is a fully managed platform as a service that lets you connect, process and react to streaming live data at ultra-low latency, powering a new class of Internet-scale apps. 
They have variety of [channels](https://www.satori.com/opendata/channels) with live streaming data. For processing data Satori provides SDK's to connect and to pull data.

**satori-wsclient** is  Streamsets origin to pull the data from satori channel using Streamsets.

### Build Instructions:
* clone the repository and build it using maven as follows

```mvn clean package -DskipTests```

* this will create a ```satori-wsclient-1.0-SNAPSHOT.tar.gz``` whcih you need to copy to ```STREAMSETS_DATACOLLECTOR-2.6.0.0/user-libs```
* restart streamsets after copying. you should see ```SatoriWs``` in origins along with other origins as following snippet 

![SatoriWS](https://github.com/Yotabites/satori-wsclient/blob/master/images/satori1.png  "SatoriWS")


### Usage:

#### Origin Configuration

* After selecting origin, you should see the SatoriWS configuration

![Origin Configuration](https://github.com/Yotabites/satori-wsclient/blob/master/images/satori2.png  "Origin Configuration")

* Enter Appkey and channel name.
* SatorWS origin gives text as output, we need to process with json processor. end-to-end pipeline would look as follows 

![SatoriWS Pipeline](https://github.com/Yotabites/satori-wsclient/blob/master/images/satori3.png  "SatoriWS Pipeline")


## Converting the String Output of Satori-WSClient to Json Format using Java Script Evaluator

### Instructions to Create a Java Script Evaluator:
* In the processors options select the Java Script Evaluator
* In the Java Script Evaluator's Configuration options,under the Javascript Window paste the following code 

  ```records[i].value=JSON.parse(records[i].value)```

* Connect the Satori-WSClient to Javascript Evaluator.

* Create any Destination and connect the Javascript Evaluator to the Destination.

* Now check the preview.You should be able to see the output similar to the one shown on the picture below.

![SatoriWS](https://github.com/Yotabites/satori-wsclient/blob/master/images/satori4.png  "SatoriWS")



**Try it out and let us know your feedback for any improvements/feature requests**

