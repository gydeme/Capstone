package ch.epfl.lts2.wikipedia

import org.rogach.scallop._

class ConfigFileOpt(args: Seq[String]) extends ScallopConf(args) with Serialization {
  // Configuration file path
  val cfgFile = opt[String](name = "config", required = true, descr = "Path to the configuration file.")

  // Optional output path for processed files
  val outputPath = opt[String](name = "outputPath", required = false, descr = "Path to output the processed data.")

  // Option to specify if ROW compression within the dumps should be handled
  val handleRowCompression = opt[Boolean](name = "handleRowCompression", default = Some(true), descr = "Enable handling of ROW_FORMAT=COMPRESSED within the Wikipedia dump files.")

  // Option for specifying the dump type explicitly
  val dumpType = opt[String](name = "dumpType", required = false, descr = "Specify the dump type (e.g., page, pagelinks). If not specified, the dump type will be inferred.")

  verify()
}

class ConfigFileOutputPathOpt(args: Seq[String]) extends ScallopConf(args) with Serialization {
  // Configuration file path
  val cfgFile = opt[String](name = "config", required = true, descr = "Path to the configuration file.")

  // Required output path for processed files
  val outputPath = opt[String](name = "outputPath", required = true, descr = "Path to output the processed data.")

  // Option to specify if ROW compression within the dumps should be handled
  val handleRowCompression = opt[Boolean](name = "handleRowCompression", default = Some(true), descr = "Enable handling of ROW_FORMAT=COMPRESSED within the Wikipedia dump files.")

  // Option for specifying the dump type explicitly
  val dumpType = opt[String](name = "dumpType", required = false, descr = "Specify the dump type (e.g., page, pagelinks). If not specified, the dump type will be inferred.")

  verify()
}
