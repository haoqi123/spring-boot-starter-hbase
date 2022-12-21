package com.spring4all.spring.boot.starter.hbase;

import com.spring4all.spring.boot.starter.hbase.api.HbaseTemplate;
import com.spring4all.spring.boot.starter.hbase.boot.HbaseProperties;
import com.spring4all.spring.boot.starter.hbase.dto.PeopleDto;
import com.spring4all.spring.boot.starter.hbase.rowmapper.PeopleRowMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.spring4all.spring.boot.starter.hbase.boot.HbaseAutoConfiguration.*;

public class HbaseTest {

    private static HbaseTemplate hbaseTemplate;

    @BeforeAll
    static void before() throws IOException {
        HbaseProperties hbaseProperties = new HbaseProperties();
        hbaseProperties.setQuorum("node01:18084,node02:18084,node03:18084");
        hbaseProperties.setRootDir("hdfs://hadoop-cluster/hbase");
        hbaseProperties.setNodeParent("/hbase");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_QUORUM, hbaseProperties.getQuorum());
        configuration.set(HBASE_ROOTDIR, hbaseProperties.getRootDir());
        configuration.set(HBASE_ZNODE_PARENT, hbaseProperties.getNodeParent());
        hbaseTemplate = new HbaseTemplate(configuration);
    }


    @Test
    void query() {
        Scan scan = new Scan();
        scan.setCaching(5000);
        List<PeopleDto> dtos = hbaseTemplate.find("bigdata:student", scan, new PeopleRowMapper());
        for (PeopleDto dto : dtos) {
            System.out.println(dto.toString());
        }

    }

    @Test
    void get() {
        PeopleDto dto = hbaseTemplate.get("bigdata:student", "1001", new PeopleRowMapper());
        System.out.println(dto.toString());
    }

    @Test
    void createNamespace() {
        System.out.println(hbaseTemplate.createNamespace("smalldata"));
    }

    @AfterAll
    static void afterAll() {
        try {
            hbaseTemplate.getConnection().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
