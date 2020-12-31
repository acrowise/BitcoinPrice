package com.ngt.etl.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;



/**
 * @author ngt
 * @create 2020-12-22 16:53
 */
public class Test {
    public static void main(String[] args) throws IOException {
        jsonToCSV("data/szt.json","data/szt.csv");
    }

    /**
     * 将原始的 json 文件数据转换成 csv 文件格式并保存
     *
     * @param jsonPath json文件路径
     * @param csvPath  csv文件保存路径
     * @throws IOException
     */
    public static void jsonToCSV(String jsonPath, String csvPath) throws IOException {
        JsonNode jsonTree = new ObjectMapper().readTree(new File(jsonPath));
        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.writerFor(JsonNode.class)
                .with(csvSchema)
                .writeValue(new File(csvPath), jsonTree);
    }
}
