package com.spring4all.spring.boot.starter.hbase.dto;

import java.util.Arrays;

public class ScannerResult {

    private String rowKey;
    private String family;
    private String qualifier;
    private byte[] value;

    public ScannerResult() {
    }

    public ScannerResult(String rowKey, String family, String qualifier, byte[] value) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "ScannerResult{" +
                "rowKey='" + rowKey + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
