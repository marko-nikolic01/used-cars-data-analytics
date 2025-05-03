val scalaVersionUsed = "2.12.18"
val sparkVersion = "3.2.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "AnalyzeVehicleColorImpactOnPriceAndDaysOnMarket",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scalaVersionUsed,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.scalameta"    %% "munit"      % "1.0.0" % Test
    )
  )
