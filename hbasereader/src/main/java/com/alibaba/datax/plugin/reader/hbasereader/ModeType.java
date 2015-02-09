package com.alibaba.datax.plugin.reader.hbasereader;

public enum ModeType {
    Normal("normal"),
    MultiVersion("multiVersion"),;


    private String mode;

    ModeType(String mode) {
        this.mode = mode;
    }

    public static boolean isNormalMode(String mode) {
        return Normal.mode.equalsIgnoreCase(mode);
    }

    public static boolean isMultiVersionMode(String mode) {
        return MultiVersion.mode.equalsIgnoreCase(mode);
    }
}
