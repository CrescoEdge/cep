package io.cresco.cep;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.UUID;

public class ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private CEPEngine cep;

    public ExecutorImpl(PluginBuilder pluginBuilder, CEPEngine cep) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(),CLogger.Level.Info);
        this.cep = cep;
    }


    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "queryadd":
                    return addCEPQuery(incoming);
                case "querydel":
                    logger.info("Clearing Streams");
                    cep.clear();
                    incoming.setParam("iscleared",Boolean.TRUE.toString());
                    return incoming;

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        logger.info("INCOMING INFO MESSAGE : " + incoming.getParams());
        //System.out.println("INCOMING INFO MESSAGE FOR PLUGIN");
        return null;
    }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        //logger.info("INCOMING EXEC MESSAGE : " + incoming.getParams());

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "queryinput":
                    queryInput(incoming);
                    break;

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }


    public MsgEvent addCEPQuery(MsgEvent incoming) {

        logger.info("Adding Stream: " + incoming.getParam("output_stream_name"));
        //System.out.println("ADD QUERY : " + incoming.getParams().toString());

        String cepId = null;

        if(incoming.getParam("query_id") == null) {
            cepId = UUID.randomUUID().toString();
            logger.info("cepId:" + incoming.getParam("query_id") + " generated");
        } else {
            cepId = incoming.getParam("query_id");
            logger.info("cepId:" + incoming.getParam("query_id") + " provided");
        }

        /*
        createQuery.setParam("input_stream_name", inputStreamName);
            createQuery.setParam("input_stream_definition", inputStreamDefinition);
            createQuery.setParam("output_stream_name", outputStreamName);
            createQuery.setParam("output_stream_definition", outputStreamDefinition);
            createQuery.setParam("query_id", cepId);
            createQuery.setParam("query", queryString);

         */

        String inputStreamName = incoming.getParam("input_stream_name");
        String inputStreamDefinition = incoming.getParam("input_stream_definition");
        String outputStreamName = incoming.getParam("output_stream_name");
        String outputStreamDefinition = incoming.getParam("output_stream_definition");
        String queryString = incoming.getParam("query");

        boolean isCreated = cep.createCEP(cepId, inputStreamName, inputStreamDefinition, outputStreamName, outputStreamDefinition, queryString);

        if(isCreated) {
            incoming.setParam("status_code","10");
            incoming.setParam("status_desc","CEP Instance Started");
        } else {
            incoming.setParam("status_code","9");
            incoming.setParam("status_desc","Could not start CEP Instance");
        }
        incoming.setParam("query_id",cepId);

        //remove body
        incoming.removeParam("input_stream_name");
        incoming.removeParam("input_stream_definition");
        incoming.removeParam("output_stream_name");
        incoming.removeParam("output_stream_definition");
        incoming.removeParam("query");
        incoming.removeParam("output_list");

        return incoming;
    }

    public void queryInput(MsgEvent incoming) {

        logger.info("Incoming Stream: " + incoming.getParam("input_stream_name") + " cepId: " + incoming.getParam("query_id"));
        //System.out.println("INCOMING: " + incoming.getParams().toString());
        //cep.input(incoming.getParam("input_stream_name"), cep.getStringPayload());
        cep.input(incoming.getParam("query_id"),incoming.getParam("input_stream_name"), incoming.getCompressedParam("input_stream_payload"));


    }

}