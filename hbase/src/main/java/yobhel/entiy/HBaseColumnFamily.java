package yobhel.entiy;

import lombok.Data;

import java.util.List;

/**
 * 类描述：TODO
 *
 * @author yezhimin
 * @date 2023-01-11 15:58
 **/
@Data
public class HBaseColumnFamily {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 行名
     */
    private String rowName;

    /**
     * 列族名
     */
    private String columnFamilyName;

    /**
     * 列集合
     */
    private List<HBaseColumn> HBaseColumns;
}
