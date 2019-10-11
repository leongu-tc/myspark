package leongu.myspark.func;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HelloUDF extends UDF {

  public String evaluate(String arg) {
    return "hello " + arg;
  }

}
