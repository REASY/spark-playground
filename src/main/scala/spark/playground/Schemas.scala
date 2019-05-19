package spark.playground

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Schemas {
  val staticPartOfMetadata: Array[StructField] = Array(
    StructField("link_id", IntegerType, false),
    StructField("length", DoubleType, false),
    StructField("capacity", DoubleType, false),
    StructField("flow_capacity", DoubleType, false),
    StructField("lanes", DoubleType, false),
    StructField("free_speed", DoubleType, false),
    StructField("FromNode_InLinksSize", IntegerType, false),
    StructField("FromNode_OutLinksSize", IntegerType, false),
    StructField("FromNode_TotalLinksSize", IntegerType, false),
    StructField("ToNode_InLinksSize", IntegerType, false),
    StructField("ToNode_OutLinksSize", IntegerType, false),
    StructField("ToNode_TotalLinksSize", IntegerType, false)
  )

  val staticPartOfLinkStat: Array[StructField] = Array(
    StructField("link_id", IntegerType, false),
    StructField("vehicle_id", StringType, false),
    StructField("travel_time", DoubleType, false),
    StructField("enter_time", DoubleType, false),
    StructField("leave_time", DoubleType, false),
    StructField("vehicles_on_road", DoubleType, false)
  )

  def metadata(maxLevel: Int): StructType = {
    val outLinkMd = (1 to maxLevel).flatMap { lvl =>
      createMetadataStatistics("OutLinks", "Length", lvl) ++
        createMetadataStatistics("OutLinks", "Capacity", lvl) ++
        createMetadataStatistics("OutLinks", "Lanes", lvl) ++
        createMetadataStatistics("OutLinks", "FreeSpeed", lvl) ++
        Array(StructField(s"L${lvl}_TotalFlowCapacity_OutLinks", DoubleType, false))
    }
    val inLinkMd = (1 to maxLevel).flatMap { lvl =>
      createMetadataStatistics("InLinks", "Length", lvl) ++
        createMetadataStatistics("InLinks", "Capacity", lvl) ++
        createMetadataStatistics("InLinks", "Lanes", lvl) ++
        createMetadataStatistics("InLinks", "FreeSpeed", lvl) ++
        Array(StructField(s"L${lvl}_TotalFlowCapacity_InLinks", DoubleType, false))
    }
    StructType(staticPartOfMetadata ++ outLinkMd ++ inLinkMd)
  }

  def linkstat(maxLevel: Int): StructType = {
    val outLinkMd = (1 to maxLevel).flatMap { lvl =>
      createLinkstatStatistics("OutLinks",  lvl)
    }
    val inLinkMd = (1 to maxLevel).flatMap { lvl =>
      createLinkstatStatistics("InLinks", lvl)
    }
    StructType(staticPartOfLinkStat ++ outLinkMd ++ inLinkMd)
  }

  private def createLinkstatStatistics(linkType: String, lvl: Int): Array[StructField] = {
    Array(
      StructField(s"L${lvl}_TotalVeh_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_MinVeh_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_MaxVeh_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_MedianVeh_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_AvgVeh_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_StdVeh_${linkType}", DoubleType, false)
    )
  }

  private def createMetadataStatistics(linkType: String, name: String, lvl: Int): Array[StructField] = {
    Array(
      StructField(s"L${lvl}_Total${name}_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_Min${name}_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_Max${name}_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_Median${name}_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_Avg${name}_${linkType}", DoubleType, false),
      StructField(s"L${lvl}_Std${name}_${linkType}", DoubleType, false)
    )
  }
}
