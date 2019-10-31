package leongu.myspark.util

import java.io.File

object myutil {

  def deleteDir(dir: File): Unit = {
    if (!dir.exists()) {
      return 
    }
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }

  def deleteDir(dir: String): Unit = {
    deleteDir(new File(dir))
  }
}
