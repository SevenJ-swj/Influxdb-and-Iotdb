package me.testdb.tool;

import lombok.Data;

import java.util.List;

@Data
public class Structure {

    private List<Pair<String,String>> fileStruct;

    private Integer hasHeader;

    private Boolean hasTime;

    private String timeFormat;

    private Integer startLine;

}
