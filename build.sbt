ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"
Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources"
lazy val sparkVersion = "3.4.3"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "5.1.0",
      "com.github.mfathi91" % "persian-date-time" % "4.2.1",
      "net.jcazevedo" %% "moultingyaml" % "0.4.2",
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2"
    )
  )
