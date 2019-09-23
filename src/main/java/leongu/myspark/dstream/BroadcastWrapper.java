package leongu.myspark.dstream;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Calendar;
import java.util.Date;

public class BroadcastWrapper {

  private Broadcast<String> var;
  private Date lastUpdatedAt = Calendar.getInstance().getTime();

  private static BroadcastWrapper obj = new BroadcastWrapper();

  private BroadcastWrapper() {
  }

  public static BroadcastWrapper getInstance() {
    return obj;
  }

  public JavaSparkContext getSparkContext(SparkContext sc) {
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
    return jsc;
  }

  public String getVarValue() {
    return var.value();
  }

  public Broadcast<String> updateAndGet(SparkContext sparkContext) {
    Date currentDate = Calendar.getInstance().getTime();
    long diff = currentDate.getTime() - lastUpdatedAt.getTime();
    if (var == null
        || diff > 6000) { //Lets say we want to refresh every 1 min = 60000 ms
      if (var != null)
        var.unpersist();
      lastUpdatedAt = new Date(System.currentTimeMillis());

      //Your logic to refresh
      String data = currentDate.toString();

      var = getSparkContext(sparkContext).broadcast(data);
    }
    return var;
  }
}