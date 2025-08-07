// --------------------- Project settings ---------------------
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

lazy val sparkVersion = "3.4.3"

// --------------------- Project definition ---------------------
lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "BNPL-ETL",
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources",
    Compile / unmanagedJars ++= {
      val libDir = baseDirectory.value / "lib"
      if (libDir.exists)
        libDir.listFiles
          .filter(_.getName.endsWith(".jar"))
          .map(Attributed.blank)
          .toSeq
      else
        Seq.empty
    },
    mainClass := Some("task.FeatureMaker"),

    // --------------------- Dependencies ---------------------
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "5.1.0",
      "com.github.mfathi91" % "persian-date-time" % "4.2.1",
      "net.jcazevedo" %% "moultingyaml" % "0.4.2",
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2",
      "com.clickhouse" % "clickhouse-jdbc" % "0.4.6",
      "io.prometheus" % "simpleclient_pushgateway" % "0.16.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.16.0",
      "io.prometheus" % "simpleclient" % "0.16.0",


    ),

    // --------------------- Assembly Settings ---------------------
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(true),
    assembly / assemblyOutputPath := baseDirectory.value / "target" / "ETL.jar",
    assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case PathList(_, "UnusedStubClass.class") => MergeStrategy.discard
      case PathList(_, "module-info.class") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
