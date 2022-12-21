package com.spring4all.spring.boot.starter.hbase.rowmapper;

import com.spring4all.spring.boot.starter.hbase.api.RowMapper;
import com.spring4all.spring.boot.starter.hbase.dto.PeopleDto;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class PeopleRowMapper implements RowMapper<PeopleDto> {

    private static byte[] COLUMNFAMILY = "info".getBytes();
    private static byte[] NAME = "name".getBytes();
    private static byte[] AGE = "age".getBytes();

    @Override
    public PeopleDto mapRow(Result result, int rowNum) throws Exception {
        PeopleDto dto = new PeopleDto();
        // TODO: 设置相关的属性值
        String name = Bytes.toString(result.getValue(COLUMNFAMILY, NAME));
//        int age = Bytes.toInt(result.getValue(COLUMNFAMILY, AGE));

        return dto.setName(name);
    }
}