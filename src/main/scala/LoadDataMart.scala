object LoadDataMart {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Error: Not enough parameters. Usage: LoadDataMart <source_path> <target_path>")
      sys.exit(1)
    }
    val sourcePath = args(0)
    val targetPath = args(1)

    DoDataMart(sourcePath, targetPath)
  }

}
