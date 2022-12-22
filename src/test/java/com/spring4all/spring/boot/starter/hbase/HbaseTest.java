package com.spring4all.spring.boot.starter.hbase;

import com.spring4all.spring.boot.starter.hbase.api.HbaseTemplate;
import com.spring4all.spring.boot.starter.hbase.boot.HbaseProperties;
import com.spring4all.spring.boot.starter.hbase.dto.PeopleDto;
import com.spring4all.spring.boot.starter.hbase.dto.ScannerResult;
import com.spring4all.spring.boot.starter.hbase.rowmapper.PeopleRowMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.spring4all.spring.boot.starter.hbase.boot.HbaseAutoConfiguration.*;

@Slf4j
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
    void createNamespace() {
        log.info("{}", hbaseTemplate.createNamespace("smalldata"));
    }

    @Test
    void put() {
        List<Mutation> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Put mutation = new Put(Bytes.toBytes("100" + i));
            mutation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("name" + i));
            mutation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(i));
            list.add(mutation);
        }

        hbaseTemplate.saveOrUpdates("bigdata:student", list);
    }

    @Test
    void findAll() {
        Scan scan = new Scan();
        scan.setCaching(5000);
        List<PeopleDto> dtos = hbaseTemplate.scan("bigdata:student", scan, new PeopleRowMapper());
        for (PeopleDto dto : dtos) {
            log.info("{}", dto.toString());
        }
    }

    @Test
    void findPart() {
        Scan scan = new Scan();
        scan.setCaching(5000);
        List<PeopleDto> dtos = hbaseTemplate.scan("bigdata:student", scan, new PeopleRowMapper());
        for (PeopleDto dto : dtos) {
            log.info("{}", dto.toString());
        }
    }

    @Test
    void getOne() {
        PeopleDto dto = hbaseTemplate.get("bigdata:student", "1003", new PeopleRowMapper());
        log.info("{}", dto.toString());
    }

    @Test
    void getByteArr() {
        byte[] nameBytes = hbaseTemplate.get("bigdata:student", "1003", "info", "name");
        log.info("{}", Bytes.toString(nameBytes));

        byte[] ageBytes = hbaseTemplate.get("bigdata:student", "1003", "info", "age");
        log.info("{}", Bytes.toInt(ageBytes));
    }

    @Test
    void scanByteArr() {
        List<byte[]> info = hbaseTemplate.scan("bigdata:student", "info", "name");
        for (byte[] bytes : info) {
            log.info("{}", Bytes.toString(bytes));
        }
    }

    @Test
    void scanByteArr2() {
        Scan scan = new Scan();
        byte[] infos = Bytes.toBytes("info");
        byte[] names = Bytes.toBytes("age");
        scan.addColumn(infos, names);
        scan.setCaching(10);
        List<byte[]> scan1 = hbaseTemplate.scan("bigdata:student", scan);
        for (byte[] bytes : scan1) {
            log.info("{}", Bytes.toInt(bytes));
        }
    }

    @Test
    void scanFilter() {
        Scan scan = new Scan();
        byte[] infos = Bytes.toBytes("info");
        byte[] names = Bytes.toBytes("age");
        scan.addColumn(infos, names);
        scan.setCaching(10);

        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                //列族名
                infos,
                //列名
                names,
                //比较关系
                CompareOperator.EQUAL,
                //值
                Bytes.toBytes(10)
        );
        filterList.addFilter(columnValueFilter);
        scan.setFilter(filterList);

        List<byte[]> scan1 = hbaseTemplate.scan("bigdata:student", scan);
        for (byte[] bytes : scan1) {
            log.info("{}", Bytes.toInt(bytes));
        }
    }

    @Test
    void scannerResult() {
        Scan scan = new Scan();
        byte[] infos = Bytes.toBytes("info");
        byte[] names = Bytes.toBytes("age");
        scan.addColumn(infos, names);
        scan.setCaching(10);

        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                //列族名
                infos,
                //列名
                names,
                //比较关系
                CompareOperator.LESS,
                //值
                Bytes.toBytes(7)
        );
        filterList.addFilter(columnValueFilter);
        scan.setFilter(filterList);

        List<ScannerResult> scannerResults = hbaseTemplate.scannerResult("bigdata:student", scan);
        for (ScannerResult scannerResult : scannerResults) {
            log.info("{}-{}-{}-{}", scannerResult.getRowKey(),
                    scannerResult.getFamily(),
                    scannerResult.getQualifier(),
                    Bytes.toInt(scannerResult.getValue()));
        }
    }

    @Test
    void getByFamilyName() {
        PeopleDto dto = hbaseTemplate.get("bigdata:student", "1003", "info", new PeopleRowMapper());
        log.info("{}", dto.toString());
    }

    @Test
    void delete() {
        List<Mutation> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Delete mutation = new Delete(Bytes.toBytes("100" + i));
            mutation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            mutation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
            list.add(mutation);
        }

        hbaseTemplate.saveOrUpdates("bigdata:student", list);
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
