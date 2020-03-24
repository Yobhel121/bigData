package cn.spark.study.core

class SecondSortKeyScala(val first: Int, val second: Int)
    extends Ordered[SecondSortKeyScala] with Serializable {
  
  def compare(that:SecondSortKeyScala):Int ={
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }

}