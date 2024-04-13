name := "big-data-project"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "2.3.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % Provided
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.4" % Provided
libraryDependencies += "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.54"

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := name.value + "-" + version.value + ".jar",
  assembly / assemblyMergeStrategy := {
    case "module-info.class" => MergeStrategy.discard
    case x => (assembly / assemblyMergeStrategy).value.apply(x)
  }
)
