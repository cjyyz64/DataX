package com.alibaba.datax.common.element;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class NullColumn extends Column {

    public NullColumn() {
        this(null);
    }

    public NullColumn(Object object) {
        super(object, Type.NULL, 0);
    }

    @Override
    public byte[] asBytes() {
        return null;
    }

    @Override
    public String asString() {
        return null;
    }

    @Override
    public Long asLong() {
        return null;

    }

    @Override
    public BigDecimal asBigDecimal() {
        return null;

    }

    @Override
    public BigInteger asBigInteger() {
        return null;

    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public Date asDate() {
        return null;
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }
}
