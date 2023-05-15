import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonTest {

  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test01")
      .master("local[*]")
      .getOrCreate()


    //读取json文件
    val df: DataFrame = spark.sqlContext.read
      .format("json")
      .json(
        "/Users/yezhimin/Downloads/api.json"
      )



    //内容展示，show默认只显示20行，可以通过参数调整
    df.select("").show()


    spark.stop()

  }

}
