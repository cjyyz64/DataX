package com.alibaba.datax.plugin.writer.zsearchwriter;

import java.io.Serializable;

/**
 * Created by discolt on 16/5/22.
 */
public class Pair<A, B> implements Serializable {

    private static final long serialVersionUID = -8015324567813735455L;
    public final A fst;
    public final B snd;

    public Pair(A var1, B var2) {
        this.fst = var1;
        this.snd = var2;
    }

    private static boolean equals(Object var0, Object var1) {
        return var0 == null && var1 == null || var0 != null && var0.equals(var1);
    }

    public static <A, B> Pair<A, B> of(A var0, B var1) {
        return new Pair(var0, var1);
    }

    public String toString() {
        return "Pair[" + this.fst + "," + this.snd + "]";
    }

    public boolean equals(Object var1) {
        return var1 instanceof Pair && equals(this.fst, ((Pair) var1).fst) && equals(this.snd, ((Pair) var1).snd);
    }

    public int hashCode() {
        return this.fst == null ? (this.snd == null ? 0 : this.snd.hashCode() + 1) : (this.snd == null ? this.fst.hashCode() + 2 : this.fst.hashCode() * 17 + this.snd.hashCode());
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);
    }

}