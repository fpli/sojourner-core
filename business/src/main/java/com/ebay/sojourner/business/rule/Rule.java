package com.ebay.sojourner.business.rule;

import java.io.IOException;

public interface Rule<T> {

    void init();

    int getBotFlag(T t) throws IOException, InterruptedException;
}
