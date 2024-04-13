object LoadDataMart {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Error: Not enough parameters. Usage: LoadDataMart <source_path> <target_path> <hive_host_port>")
      sys.exit(1)
    }
    val sourcePath = args(0)
    val targetPath = args(1)
    val hiveHostPort = args(2)

    DoDataMart(sourcePath, targetPath, hiveHostPort)
  }

}
