package io.cresco.cep;


import io.cresco.library.agent.AgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

@Component(
        service = { PluginService.class },
        scope=ServiceScope.PROTOTYPE,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        servicefactory = true,
        reference=@Reference(name="io.cresco.library.agent.AgentService", service=AgentService.class)
)

public class Plugin implements PluginService {

    public BundleContext context;
    public PluginBuilder pluginBuilder;
    private Executor executor;
    private CLogger logger;
    private Map<String, Object> map;
    public String myname;
    public CEPEngine cep;
    private Thread messageSenderThread = null;

    @Activate
    void activate(BundleContext context, Map<String, Object> map) {

        this.context = context;
        this.map = map;
        myname = "this is my name";

    }


    @Modified
    void modified(BundleContext context, Map<String, Object> map) {
        System.out.println("Modified Config Map PluginID:" + (String) map.get("pluginID"));
    }

    @Override
    public boolean inMsg(MsgEvent incoming) {

        pluginBuilder.msgIn(incoming);
        return true;
    }

    @Deactivate
    void deactivate(BundleContext context, Map<String,Object> map) {

        isStopped();
        this.context = null;
        this.map = null;

    }

    @Override
    public boolean isStarted() {


        try {
            pluginBuilder = new PluginBuilder(this.getClass().getName(), context, map);
            this.logger = pluginBuilder.getLogger(Plugin.class.getName(), CLogger.Level.Info);

            this.cep = new CEPEngine(pluginBuilder);

            this.executor = new ExecutorImpl(pluginBuilder, cep);
            pluginBuilder.setExecutor(executor);

            while (!pluginBuilder.getAgentService().getAgentState().isActive()) {
                logger.info("Plugin " + pluginBuilder.getPluginID() + " waiting on Agent Init");
                Thread.sleep(1000);
            }

            /*
            MessageSender messageSender = new MessageSender(pluginBuilder);
            messageSenderThread = new Thread(messageSender);
            messageSenderThread.start();
            */

            pluginBuilder.setIsActive(true);

            logger.info("Started CDP PluginID:" + (String) map.get("pluginID"));

            return true;

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean isStopped() {
        try {

            pluginBuilder.setIsActive(false);

            logger.debug("PRE JOIN! " + pluginBuilder.getPluginID() + " isActive:" + pluginBuilder.isActive());
            if(messageSenderThread != null) {
                messageSenderThread.join();
            }
            logger.debug("POST JOIN! " + pluginBuilder.getPluginID() + " isActive:" + pluginBuilder.isActive());

            if(cep != null) {
                cep.shutdown();
            }

            return true;

        } catch(Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("isStopped: " + errors.toString());
            return false;
        }

    }
}