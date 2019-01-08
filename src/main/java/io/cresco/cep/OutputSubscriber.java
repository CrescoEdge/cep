package io.cresco.cep;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private PluginBuilder plugin;
    private CLogger logger;

    private Schema schema;
    private String topic;
    private String streamName;
    private DatumReader<GenericData.Record> reader;

    private String cepId;


    public OutputSubscriber(PluginBuilder pluginBuilder, String cepId, Schema schema, String topic, String streamName) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(OutputSubscriber.class.getName(),CLogger.Level.Info);

        this.cepId = cepId;

        this.schema = schema;
        this.topic = topic;
        this.streamName = streamName;


        reader = new GenericDatumReader<>(schema);
    }

    @Override
    public void onMessage(Object msg) {

        try {
            ByteBuffer bb = (ByteBuffer) msg;
            Decoder decoder = new DecoderFactory().binaryDecoder(bb.array(), null);
            GenericData.Record rec = reader.read(null, decoder);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
            DatumWriter<GenericData.Record> writer = new ReflectDatumWriter<>(schema);
            writer.write(rec, encoder);
            encoder.flush();

            String input = new String(outputStream.toByteArray());
           // System.out.println("Original Object Schema JSON: " + schema);
           // System.out.println("Original Object DATA JSON: "+ input);


            logger.error(input);
            TextMessage tm = plugin.getAgentService().getDataPlaneService().createTextMessage();
            tm.setText(input);
            tm.setStringProperty("stream_name",streamName);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,tm);




            /*
            if(!outputList.isEmpty()) {

                for(Map<String,String> outputMap : outputList) {

                    String outputRegion = outputMap.get("region");
                    String outputAgent = outputMap.get("agent");
                    String outputPlugin = outputMap.get("pluginid");

                    logger.info("Sending out " + outputRegion + " " + outputAgent + " " + outputPlugin + " " + input);

                    MsgEvent inputMsg = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, outputRegion, outputAgent, outputPlugin);
                    inputMsg.setParam("action", "queryinput");
                    inputMsg.setParam("input_stream_name", streamName);
                    inputMsg.setCompressedParam("input_stream_payload", input);
                    plugin.msgOut(inputMsg);
                }

            }
            */


        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
