import sbt.*

object Dependencies {
  object Versions {
    // Supported version by Dataflow at https://cloud.google.com/dataflow/docs/support/sdk-version-support-status
    val beam = "2.48.0"

    // Apache Beam compatibility with scio list at https://spotify.github.io/scio/releases/Apache-Beam.html
    val scio = "0.13.0"

    val protobuf = "3.21.12"
  }

  object Libs {
    val beam = Seq(
      "org.apache.beam" % "beam-sdks-java-core" % Versions.beam,

      // GCP and Dataflow runners
      // "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % Versions.beam,
      // "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % Versions.beam,

      // We are using the local runner for now instead of the dataflow runner
      "org.apache.beam" % "beam-runners-direct-java" % Versions.beam,

      // IOs
      "org.apache.beam" % "beam-sdks-java-io-mqtt" % Versions.beam
    )

    val scio = Seq(
      "com.spotify" %% "scio-core" % Versions.scio,
      "com.spotify" %% "scio-extra" % Versions.scio,
      "com.spotify" %% "scio-test" % Versions.scio % Test
    )

    val common = Seq(
      "org.slf4j" % "slf4j-simple" % "1.7.36",
      "com.google.protobuf" % "protobuf-java" % Versions.protobuf,
      "com.google.protobuf" % "protobuf-java" % Versions.protobuf % "protobuf"
    )
    val all = beam ++ scio ++ common
  }
}
