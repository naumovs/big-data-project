object ExportToExternalStorage {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println(
        """Error: Not enough parameters.
          |Usage: ExportToExternalStorage <source_path> <target_host> <target_db.target_table> <target_orderby>"""
          .stripMargin)
      sys.exit(1)
    }
    val sourcePath = args(0)
    val targetHost = args(1)
    val targetDBAndTable = args(2)
    val targetOrderBy = args(3)

    DoExportToExternalStorage(sourcePath, targetHost, targetDBAndTable, targetOrderBy)
  }

}
