ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val sparkVersion = "3.4.3"

lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin) // <<-- IMPORTANT
  .settings(
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources",
    mainClass := Some("task.FeatureMaker"), // <<-- Move mainClass here inside settings
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "5.1.0",
      "com.github.mfathi91" % "persian-date-time" % "4.2.1",
      "net.jcazevedo" %% "moultingyaml" % "0.4.2",
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2"
    ),
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(true),
    assembly / assemblyOutputPath := new File(baseDirectory.value, "target/ETL.jar"),
    assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last == "UnusedStubClass.class" => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
