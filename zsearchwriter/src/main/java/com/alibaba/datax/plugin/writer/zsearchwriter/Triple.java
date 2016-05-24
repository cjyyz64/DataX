package com.alibaba.datax.plugin.writer.zsearchwriter;

import java.io.Serializable;

/**
 * Created by discolt on 16/5/22.
 */
public class Triple<A, B,C> implements Serializable {

    private static final long serialVersionUID = -8015324567813735455L;
    public final A fst;
    public final B snd;
    public final C trd;

    public Triple(A var1, B var2, C var3) {
        this.fst = var1;
        this.snd = var2;
        this.trd = var3;
    }

    private static boolean equals(Object var0, Object var1) {
        return var0 == null && var1 == null || var0 != null && var0.equals(var1);
    }

    public static <A, B,C> Triple of(A var0, B var1, C var3) {
        return new Triple(var0, var1,var3);
    }

    public String toString() {
        return "Triple[" + this.fst + "," + this.snd +","+this.trd+ "]";
    }

    public boolean equals(Object var1) {
        return var1 instanceof Triple && equals(this.fst, ((Triple) var1).fst) && equals(this.snd, ((Triple) var1).snd)&& equals(this.trd, ((Triple) var1).trd);
    }

    public int hashCode() {
        return  (fst == null ? 0 : fst.hashCode()) ^
                (snd == null ? 0 : snd.hashCode()) ^
                (trd == null ? 0 : trd.hashCode());
    }


}