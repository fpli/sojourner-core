package com.ebay.sojourner.ubd.rt.operators.session;

import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.model.UbiSession;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.apache.flink.types.Either.Right;

@Slf4j
public class DetectableMapFunction extends RichMapFunction<UbiSession, Either<UbiEvent, UbiSession>> {

    @Override
    public void open(Configuration conf) throws Exception {

    }

    @Override
    public Either<UbiEvent, UbiSession> map( UbiSession value) throws Exception {

        return Either.Right(value);
    }
}