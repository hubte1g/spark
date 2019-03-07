val columns = DF.schema.fields.map(_.name)

    val selectiveDifferences = columns.map(col =>
      DF.select(col).except(otherDF.select(col)))

    selectiveDifferences.map(diff => {if(diff.count > 0) diff.show})
