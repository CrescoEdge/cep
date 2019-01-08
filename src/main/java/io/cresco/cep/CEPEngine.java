package io.cresco.cep;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class CEPEngine {

    private PluginBuilder plugin;
    private CLogger logger;

    private SiddhiManager siddhiManager;
    private Map<String,CEPInstance> cepMap;
    private AtomicBoolean lockCEP = new AtomicBoolean();


    public CEPEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPEngine.class.getName(),CLogger.Level.Info);

        cepMap = Collections.synchronizedMap(new HashMap<>());

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();

        try {
            InMemorySink sink = new InMemorySink();
            sink.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public CEPInstance getCEPInstance(String cepId) {
        CEPInstance cepInstance = null;
        try {
            synchronized (lockCEP) {
                if(cepMap.containsKey(cepId)) {
                    cepInstance = cepMap.get(cepId);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return cepInstance;
    }

    public boolean removeCEP(String cepId) {
        boolean isRemoved = false;
        synchronized (lockCEP) {
            if(cepMap.containsKey(cepId)) {
                cepMap.get(cepId).shutdown();
            }
            cepMap.remove(cepId);
            isRemoved = true;
        }
        return isRemoved;
    }

    public void shutdown() {
        try {

            synchronized (lockCEP) {
                for (Map.Entry<String, CEPInstance> entry : cepMap.entrySet()) {
                    //String key = entry.getKey();
                    CEPInstance value = entry.getValue();
                    value.shutdown();
                }
            }

            if(siddhiManager != null) {
                siddhiManager.shutdown();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void clear() {
        try {
            synchronized (lockCEP) {
                for (Map.Entry<String, CEPInstance> entry : cepMap.entrySet()) {
                    //String key = entry.getKey();
                    CEPInstance value = entry.getValue();
                    value.clear();
                }
            }

            if(siddhiManager != null) {
                siddhiManager.shutdown();
                siddhiManager = new SiddhiManager();
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
    public String createCEP(String inputRecordSchemaString, String inputStreamName, String outputStreamName, String outputStreamAttributesString,String queryString) {

        String cepId = null;
        try {
            cepId = UUID.randomUUID().toString();
            if(!createCEP(cepId,inputRecordSchemaString,inputStreamName,outputStreamName,outputStreamAttributesString,queryString)) {
                cepId = null;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            cepId = null;
        }
        return cepId;
    }
    */
        public boolean createCEP(String cepId, String inputRecordSchemaString, String inputStreamName, String outputStreamName, String outputStreamAttributesString,String queryString) {

        boolean isCreated = false;
        try {

            CEPInstance cepInstance = new CEPInstance(plugin,siddhiManager,cepId,inputRecordSchemaString,inputStreamName,outputStreamName,outputStreamAttributesString,queryString);

            synchronized (lockCEP) {
                cepMap.put(cepId,cepInstance);
            }
            isCreated = true;

            } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isCreated;
    }

    public void input(String cepId, String streamName, String jsonPayload) {
        try {

            synchronized (lockCEP) {
                if(cepMap.containsKey(cepId)) {
                    cepMap.get(cepId).input(streamName, jsonPayload);
                } else {
                    logger.error("cepId: " + cepId + " does not exist!");
                }
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

}
