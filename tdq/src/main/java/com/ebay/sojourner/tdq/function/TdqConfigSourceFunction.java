package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.TdqConfigMapping;
import com.ebay.sojourner.common.util.RestClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

@Slf4j
public class TdqConfigSourceFunction extends RichSourceFunction<TdqConfigMapping> {

    private RestClient restClient;
    private ObjectMapper objectMapper;
    private final String baseURL;
    private final Long interval;
    private final String env;

    public TdqConfigSourceFunction(String baseURL, Long interval, String env) {
        this.baseURL = baseURL;
        this.interval = interval;
        this.env = env;
    }

    @Override
    public void run(SourceContext<TdqConfigMapping> ctx) throws Exception {
        while (true) {
            try {
                Response response = restClient.get(
                        "/api/custom_topic_config/list/topic_page_ids?env=" + env);
                String responseResult="[\n" +
                        "    {\n" +
                        "        \"createdBy\": \"xiaoding\",\n" +
                        "        \"updatedBy\": \"xiaoding\",\n" +
                        "        \"createTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"updateTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"metricName\":\"Glabal_Mandotory_Tag_Rate\",\n" +
                        "        \"metricType\":\"1\",\n" +
                        "        \"pageFamilys\":[\"ASQ\"\n" +
                        "           ,\"BID\"\n" +
                        "           ,\"BIDFLOW\"\n" +
                        "           ,\"BIN\"\n" +
                        "           ,\"BINFLOW\"\n" +
                        "           ,\"CART\"\n" +
                        "           ,\"OFFER\"\n" +
                        "           ,\"UNWTCH\"\n" +
                        "           ,\"VI\"\n" +
                        "            ,\"WTCH\"\n" +
                        "            ,\"XO\"],\n" +
                        "        \"tags\":[\"itm|itmid|itm_id|itmlist|litm\",\"u\"],\n" +
                        "        \"pageIds\": [],\n" +
                        "        \"env\": \"prod\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "        \"createdBy\": \"xiaoding\",\n" +
                        "        \"updatedBy\": \"xiaoding\",\n" +
                        "        \"createTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"updateTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"metricName\":\"Event_Capature_Publish_Latency\",\n" +
                        "        \"metricType\":\"2\",\n" +
                        "        \"pageFamilys\":[],\n" +
                        "        \"tags\":[\"TDuration\"],\n" +
                        "        \"pageIds\": [],\n" +
                        "        \"env\": \"prod\"\n" +
                        "    },\n" +
                        "     {\n" +
                        "        \"createdBy\": \"xiaoding\",\n" +
                        "        \"updatedBy\": \"xiaoding\",\n" +
                        "        \"createTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"updateTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"metricName\":\"Marketing_Event_Volume\",\n" +
                        "        \"metricType\":\"3\",\n" +
                        "        \"pageFamilys\":[],\n" +
                        "        \"tags\":[],\n" +
                        "        \"pageIds\": [2547208,2483445],\n" +
                        "        \"env\": \"prod\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "        \"createdBy\": \"xiaoding\",\n" +
                        "        \"updatedBy\": \"xiaoding\",\n" +
                        "        \"createTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"updateTime\": \"2021-02-08T07:53:49\",\n" +
                        "        \"metricName\":\"Transformation_Error_Rate \",\n" +
                        "        \"metricType\":\"4\",\n" +
                        "        \"pageFamilys\":[],\n" +
                        "        \"tags\":[\"u-Integer\"],\n" +
                        "        \"pageIds\": [],\n" +
                        "        \"env\": \"prod\"\n" +
                        "    }\n" +
                        "]";
                List<TdqConfigMapping> tdqConfigMappings =
                        objectMapper
                                .reader()
                                .forType(new TypeReference<List<TdqConfigMapping>>() {})
                            .readValue(responseResult);// test in local
                //                                .readValue(response.body().string());

                for (TdqConfigMapping mapping : tdqConfigMappings) {
                    ctx.collect(mapping);
                }
            } catch (Exception e) {
                log.error("Error when calling rest api");
            }

            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        log.info("MappingSourceFunction cancelled");
        restClient = null;
        objectMapper = null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.restClient = new RestClient(baseURL);
        this.objectMapper = new ObjectMapper();
    }
}
