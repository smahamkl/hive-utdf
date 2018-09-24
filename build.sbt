import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "hive.scala.udtf",
      scalaVersion := "2.12.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "hive-udtf",
    libraryDependencies += "org.apache.hive" % "hive-exec" % "2.1.1",
    libraryDependencies += scalaTest % Test,
    resolvers += "Spring Plugins" at "http://repo.spring.io/plugins-release/",
    mainClass in assembly := Some("hive.scala.udtf.ExpandTree3UDTF"),
    assemblyJarName in assembly := "hive-udtf.jar"
  )

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)               => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml"        => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types"      => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class"      => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".gif"        => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "version.txt" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "VERSION.txt" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "Plugins.dat" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "defaultManifest.mf" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.discard
  case "application.conf"                                  => MergeStrategy.concat
  case "unwanted.txt"                                      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
