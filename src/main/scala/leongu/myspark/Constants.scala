package leongu.myspark

import java.io.File

object Constants {
  val projPath: String = new File(Thread.currentThread.getContextClassLoader.getResource("").getPath).getParentFile.getParentFile.getParentFile.getPath
  val prefix: String = projPath + "/src/main/resources/"

}
