package io.cresco.cep;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.wso2.extension.siddhi.map.avro.util.schema.RecordSchema;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;
import org.wso2.siddhi.query.api.definition.Attribute;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CEPInstance {

    private PluginBuilder plugin;
    private CLogger logger;

    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;

    private Map<String,Schema> schemaMap;
    private Map<String,String> topicMap;

    private AtomicBoolean lockSchema = new AtomicBoolean();
    private AtomicBoolean lockTopic = new AtomicBoolean();

    private String cepId;


    public CEPInstance(PluginBuilder pluginBuilder, SiddhiManager siddhiManager, String cepId, String inputRecordSchemaString, String inputStreamName, String outputStreamName, String outputStreamAttributesString,String queryString) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPInstance.class.getName(),CLogger.Level.Info);

        schemaMap = Collections.synchronizedMap(new HashMap<>());
        topicMap = Collections.synchronizedMap(new HashMap<>());

        this.siddhiManager = siddhiManager;

        this.cepId = cepId;


        try {


            String inputTopic = UUID.randomUUID().toString();
            String outputTopic = UUID.randomUUID().toString();

            synchronized (lockTopic) {
                topicMap.put(inputStreamName, inputTopic);
                topicMap.put(outputStreamName, outputTopic);
            }

            Schema.Parser parser = new Schema.Parser();
            Schema inputSchema = parser.parse(inputRecordSchemaString);

            synchronized (lockSchema) {
                schemaMap.put(inputStreamName, inputSchema);
            }

            String sourceString = getSourceString(inputSchema, inputTopic, inputStreamName);
            String sinkString = getSinkString(outputTopic,outputStreamName,outputStreamAttributesString);

            //Generating runtime
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sourceString + " " + sinkString + " " + queryString);

            Schema outputSchema = getRecordSchema(outputStreamName);

            synchronized (lockSchema) {
                schemaMap.put(outputStreamName, outputSchema);
            }


            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {


                        if (msg instanceof TextMessage) {

                            //System.out.println(RXQueueName + " msg:" + ((TextMessage) msg).getText());
                            InMemoryBroker.publish(inputTopic, getByteGenericDataRecordFromString(inputRecordSchemaString, ((TextMessage) msg).getText()));
                            //String message = ((TextMessage) msg).getText();
                            //logger.error("YES!!! " + message);

                        }
                    } catch(Exception ex) {

                        ex.printStackTrace();
                    }
                }
            };

            pluginBuilder.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"stream_name='" + inputStreamName + "'");


            InMemoryBroker.Subscriber subscriberTest = new OutputSubscriber(plugin,cepId,outputSchema,outputTopic,outputStreamName);

            //subscribe to "inMemory" broker per topic
            InMemoryBroker.subscribe(subscriberTest);

            //Starting event processing
            siddhiAppRuntime.start();

        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public void shutdown() {
        try {

            if(siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

            /*
            if(siddhiManager != null) {
                siddhiManager.shutdown();
            }
            */

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void clear() {
        try {

            if(siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
                siddhiAppRuntime = null;
            }

            /*
            if(siddhiManager != null) {
                siddhiManager.shutdown();
                siddhiManager = new SiddhiManager();
            }
            */

            synchronized (lockSchema) {
                schemaMap.clear();
            }

            synchronized (lockTopic) {
                topicMap.clear();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void input(String streamName, String jsonPayload) {
        try {

            String topicName = null;
            synchronized (lockTopic) {
                if(topicMap.containsKey(streamName)) {
                    topicName = topicMap.get(streamName);
                }
            }

            Schema schema = null;

            synchronized (lockSchema) {
                if(schemaMap.containsKey(streamName)) {
                    schema = schemaMap.get(streamName);
                }
            }

                if ((topicName != null) && (schema != null)) {
                //start measurement
                    InMemoryBroker.publish(topicName, getByteGenericDataRecordFromString(schema, jsonPayload));
                } else {
                    System.out.println("input error : no schema");
                }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public String getStringPayload() {

        String rec = null;

        try{

            String source = "mysource";
            String urn = "myurn";
            String metric = "mymetric";
            long ts = System.currentTimeMillis();

            Random r = new Random();
            double value = r.nextDouble();

            Ticker tick = new Ticker(source, urn, metric, ts, value);

            Schema schema = ReflectData.get().getSchema(Ticker.class);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
            DatumWriter<Ticker> writer = new ReflectDatumWriter<>(schema);
            writer.write(tick, encoder);
            encoder.flush();

            rec = new String(outputStream.toByteArray());

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return rec;
    }

    private GenericData.Record getGenericPayload() {

        GenericData.Record rec = null;

        try{

            String source = "mysource";
            String urn = "myurn";
            String metric = "mymetric";
            long ts = System.currentTimeMillis();

            Random r = new Random();
            double value = r.nextDouble();

            Ticker tick = new Ticker(source, urn, metric, ts, value);

            Schema schema = ReflectData.get().getSchema(Ticker.class);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
            DatumWriter<Ticker> writer = new ReflectDatumWriter<>(schema);
            writer.write(tick, encoder);
            encoder.flush();

            String input = new String(outputStream.toByteArray());
            Decoder decoder = new DecoderFactory().jsonDecoder(schema, input);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            rec = reader.read(null, decoder);


        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return rec;
    }

    private byte[] getByteGenericDataRecordFromString(Schema schema, String jsonInputPayload) {

        byte[] bytes = null;
        try{

            Decoder decoder = new DecoderFactory().jsonDecoder(schema, jsonInputPayload);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            GenericData.Record rec = reader.read(null, decoder);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericData.Record> writer = new SpecificDatumWriter<>(schema);

            writer.write(rec, encoder);
            encoder.flush();
            out.close();
            bytes = out.toByteArray();

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return bytes;
    }

    private byte[] getByteGenericDataRecordFromString(String schemaString, String jsonInputPayload) {

        byte[] bytes = null;
        try{

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaString);

            Decoder decoder = new DecoderFactory().jsonDecoder(schema, jsonInputPayload);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            GenericData.Record rec = reader.read(null, decoder);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericData.Record> writer = new SpecificDatumWriter<>(schema);

            writer.write(rec, encoder);
            encoder.flush();
            out.close();
            bytes = out.toByteArray();

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return bytes;
    }

    private byte[] getBytePayload() {

        byte[] bytes = null;
        try{

            Schema schema = ReflectData.get().getSchema(Ticker.class);
            GenericData.Record rec = getGenericPayload();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericData.Record> writer = new SpecificDatumWriter<>(schema);

            writer.write(rec, encoder);
            encoder.flush();
            out.close();
            bytes = out.toByteArray();


        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return bytes;
    }

    public Schema getSchema(String streamName) {
        Schema returnSchema = null;
        try {

            synchronized (lockSchema) {
                if (schemaMap.containsKey(streamName)) {
                    returnSchema = schemaMap.get(streamName);
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return returnSchema;
    }

    private Schema getRecordSchema(String streamName) {
        Schema returnSchema = null;
        try {

            Collection<List<Sink>> sinkCollectionList = siddhiAppRuntime.getSinks();
            for (Iterator<List<Sink>> iterator = sinkCollectionList.iterator(); iterator.hasNext();) {
                List<Sink> sinkList = iterator.next();

                for (Sink sink : sinkList) {

                    if(sink.getStreamDefinition().getId().equals(streamName)) {
                        List<Attribute> attributeList = sink.getStreamDefinition().getAttributeList();
                        RecordSchema recordSchema = new RecordSchema();
                        returnSchema = recordSchema.generateAvroSchema(attributeList, streamName);
                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return returnSchema;
    }

    private String getSourceString(Schema schema, String topic, String streamName) {
        String sourceString = null;
        try {

            StringBuilder sb = new StringBuilder();
            List<Schema.Field> fieldList = schema.getFields();
            for(Schema.Field field : fieldList) {
                sb.append(field.name() + " " + field.schema().getType().getName() + ", ");
            }

            sourceString  = "@source(type='inMemory', topic='" + topic + "', @map(type='avro', schema .def = \"\"\"" + schema  + "\"\"\")) " +
                    "define stream " + streamName + " (" + sb.substring(0,sb.length() -2) + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sourceString;
    }

    private String getSinkString(String topic, String streamName, String outputSchemaString) {
        String sinkString = null;
        try {

            sinkString = "@sink(type='inMemory', topic='" + topic + "', @map(type='avro')) " +
                    "define stream " + streamName + " (" + outputSchemaString + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sinkString;
    }

}
