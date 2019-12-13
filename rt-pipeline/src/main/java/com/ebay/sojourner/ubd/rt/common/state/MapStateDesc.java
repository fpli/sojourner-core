package com.ebay.sojourner.ubd.rt.common.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Set;

public class MapStateDesc {

    public static final MapStateDescriptor<String,Set<Integer>> ipSignatureDesc = new MapStateDescriptor<>(
            "broadcast-ipSignature-state", BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(new TypeHint<Set<Integer>>() {})
    );

    public static final MapStateDescriptor<String, Set<Integer>> agentSignatureDesc = new MapStateDescriptor<>(
            "broadcast-agentSignature-state", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Set<Integer>>() {})
    );

    public static final MapStateDescriptor<String,Set<Integer>> agentIpSignatureDesc = new MapStateDescriptor<>(
            "broadcast-agentIpSignature-state", BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(new TypeHint<Set<Integer>>() {})
    );

    static {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        ipSignatureDesc.enableTimeToLive(ttlConfig);
        agentSignatureDesc.enableTimeToLive(ttlConfig);
        agentIpSignatureDesc.enableTimeToLive(ttlConfig);
    }
}
