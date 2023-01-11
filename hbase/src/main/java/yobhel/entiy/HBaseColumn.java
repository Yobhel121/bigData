package yobhel.entiy;

import lombok.Data;

/**
 * 类描述：TODO
 *
 * @author yezhimin
 * @date 2023-01-11 15:14
 **/
@Data
public class HBaseColumn {
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
     * 列名
     */
    private String columnName;

    /**
     * 列值
     */
    private String value;
}
