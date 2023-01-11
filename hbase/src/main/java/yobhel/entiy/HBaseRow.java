package yobhel.entiy;

import lombok.Data;

import java.util.List;

/**
 * 类描述：TODO
 *
 * @author yezhimin
 * @date 2023-01-11 16:03
 **/
@Data
public class HBaseRow {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 行名
     */
    private String rowName;

    /**
     * 同行的列族集合
     */
    private List<HBaseColumnFamily> HBaseColumnFamilies;

}
