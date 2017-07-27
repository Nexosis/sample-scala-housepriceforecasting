package com.nexosis.samples

import com.nexosis.impl.{NexosisClient, NexosisClientException}
import com.nexosis.model._
import java.io.{File, FileNotFoundException, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import org.jfree.chart.{ChartFactory, ChartUtilities}
import org.jfree.chart.axis.{DateAxis, DateTickUnit, DateTickUnitType}
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.time.{Day, RegularTimePeriod, TimeSeries, TimeSeriesCollection}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.io.{Source, StdIn}
import scala.util.Try

object ForecastHomePrices {
  private val path = System.getProperty("user.dir") + "/data"

  def main(args: Array[String]): Unit = {
    val sourceFile = path + "/State_Zhvi_BottomTier.csv"
    //val timestamp: Long = System.currentTimeMillis / 1000
    val timestamp : Long = 1500911996
    val dataSetNameSuffix = s"-housedata-${timestamp.toString}"

    val skipDataProcessing = true
    val skipCreateSession = true

    //deleteAllHousingData(client)

    val bufferedSource = Source.fromFile(sourceFile).getLines

    val client = new NexosisClient(
      sys.env("NEXOSIS_API_KEY"),
      sys.env("NEXOSIS_BASE_TEST_URL")
    )

    try {
      if (!skipDataProcessing) {
        buildDatasets(client, bufferedSource, dataSetNameSuffix)
      }

      var sessionList = new util.ArrayList[UUID]

      // Estimate Cost of forcasting on all datasets
      var totalEstimatedCost = estimateForecastSessions(client, dataSetNameSuffix)

      println(s"Estimated cost of forceasting is: ${totalEstimatedCost}")
      println(s"continue? Type 'y' for (y)es or anything else for no.")

      val input = StdIn.readLine()
      if (!input.equals("y")) {
        System.exit(1)
      }

      if (!skipCreateSession) {
        sessionList = createForecastSessions(client, dataSetNameSuffix)
      } else {
        sessionList = loadSavedSessionIds()
      }

      waitAndPlotSessionResults(client, sessionList)
    } catch {
      case nce: NexosisClientException => {
        println(s"Status: ${nce.getStatusCode}")
        println(s"Status: ${nce.getMessage}")
        if (nce.getErrorResponse != null && nce.getErrorResponse.getErrorDetails != null) {
          nce.getErrorResponse.getErrorDetails.entrySet.forEach { entry =>
            println(entry.getKey + " " + entry.getValue)
          }
        }
        println(s"Error Response: ${nce.getErrorResponse}")
      }
    }
  }

  private def waitAndPlotSessionResults(client: NexosisClient, sessionList: util.ArrayList[UUID]) = {
    // Wait for all sessions to finish.
    sessionList.forEach { id =>
      var status = client.getSessions.getStatus(id)

      print("Waiting for " + id.toString + ".")
      while ((status.getStatus == SessionStatus.STARTED)
        || (status.getStatus == SessionStatus.REQUESTED))  {
        Thread.sleep(2000)
        print(".")
        status = client.getSessions.getStatus(id)
      }
      println(".")
      // Retrieve session data
      var results: SessionResult = new SessionResult
      results = client.getSessions.getResults(id)
      // Retrieve historical data
      val dataset = client.getDataSets.get(results.getDataSetName, 0, 300, new util.ArrayList[String])
      // Build plot using historical and prediction and save it to disk
      plotTimeSeries(dataset, results)
    }
  }

  private def loadSavedSessionIds(): util.ArrayList[UUID] = {
    val sessionList = new util.ArrayList[UUID]
    // Load from saved sessionList file.
    try {
      val idFiles = path + "/sessionlist--housedata-1500911996.txt"
      val sessionIdList = Source.fromFile(idFiles).getLines
      while (sessionIdList.hasNext) {
        sessionList.add(UUID.fromString(sessionIdList.next))
      }
    } catch {
      case fnfe: FileNotFoundException => {
        println(fnfe.getMessage)
      }
    }
    return sessionList
  }

  private def createForecastSessions(client: NexosisClient, dataSetNameSuffix: String): util.ArrayList[UUID] = {
    val sessionList = new util.ArrayList[UUID]
    // Enumerate datasets with the provided filter
    val dataSetList = client.getDataSets.list(dataSetNameSuffix)
    using(new PrintWriter(new FileOutputStream(path + s"/sessionlist-${dataSetNameSuffix}.txt", true), true)) { pw =>
      dataSetList.getItems.forEach { item =>
        // Create Forecast for the next 6 months (6 predictions points per dataset)
        val session = client.getSessions.createForecast(
          item.getDataSetName(),
          "cost",
          DateTime.parse("2017-06-01T00:00:00Z"),
          DateTime.parse("2017-12-01T00:00:00Z"),
          ResultInterval.MONTH
        )

        // Collect all the session ID's to check status
        val sessionId = session.getSessionId()
        sessionList.add(sessionId)
        // save em for later
        pw.write(sessionId.toString + sys.props("line.separator"))
      }
    }
    return sessionList
  }

  private def estimateForecastSessions(client: NexosisClient, dataSetNameSuffix: String): BigDecimal = {
    // Enumerate datasets with the provided filter
    val dataSetList = client.getDataSets.list(dataSetNameSuffix)

    var totalCost : BigDecimal = 0.0
    dataSetList.getItems.forEach { item =>
      // Estimate Forecast for the next 6 months (6 predictions points per dataset)
      val estimate = client.getSessions.estimateForecast(
        item.getDataSetName(),
        "cost",
        DateTime.parse("2017-06-01T00:00:00Z"),
        DateTime.parse("2017-12-01T00:00:00Z"),
        ResultInterval.MONTH
      ).asInstanceOf[ReturnsCost]

      println(s"${item.getDataSetName} costs ${estimate.getCost.getCurrency.getSymbol} ${estimate.getCost.getAmount} ")
      totalCost += estimate.getCost.getAmount
    }
    return totalCost
  }

  private def buildDatasets(client: NexosisClient, bufferedSource: Iterator[String], dataSetNameSuffix: String) = {
    // Extract the dates off of the first row / header columns in the CSV
    // and build a dataSetData object with all the dates
    val dataSetData = ExtractHeadersAndDates(bufferedSource)

    // Now loop over the rest of the rows to get each state's data row
    while (bufferedSource.hasNext) {
      val cells = bufferedSource.next.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      // Populate / Replace dataSetData cost data with data from the cells in the next row
      getRegionalData(dataSetNameSuffix, dataSetData, cells)

      // DataSet Data is complete, upload...
      client.getDataSets.create(dataSetData.getDataSetName, dataSetData)
    }
  }

  private def deleteAllHousingData(client :NexosisClient) = {
    // Delete all house data and sessions
    client.getDataSets.list().getItems.forEach { item =>
      if (item.getDataSetName.contains("-housedata-")) {
        client.getDataSets.remove(item.getDataSetName, DataSetDeleteOptions.CASCASE_BOTH)
      }
    }
  }

  private def getRegionalData(dataSetNameSuffix: String, dataSetData: DataSetData, cells: Array[String]) = {
    val regionData: util.List[util.Map[String, String]] = dataSetData.getData

    // Update DataSet name based on value in first position of Cells array
    val dataSetName = cells(1).replace("\"", "").replace(" ", "_").toLowerCase() + dataSetNameSuffix;
    dataSetData.setDataSetName(dataSetName)

    // println(cells(0)) // column 0 is RegionID - not needed
    // println(cells(2)) // column 2 is SizeRank - not needed

    var count = 0
    cells.foreach { cell =>
      // ignore first 3 columns since they've been handled above
      if (count >= 3) {
        val costs: util.Map[String, String] =
          new util.HashMap[String, String]()
        costs.put("cost", cell)
        regionData.get(count - 3).put("cost", cell)
      }
      count += 1
    }
  }

  private def ExtractHeadersAndDates(bufferedSource: Iterator[String]): DataSetData = {
    // grab the dates from the rest of the header columns.
    val dateFormatter = DateTimeFormat.forPattern("yyyy-MM")
    val dataSetData: DataSetData = new DataSetData

    val regionData: util.List[util.Map[String, String]] =
      new util.ArrayList[util.Map[String, String]]()

    SetupColumnsMetadata(dataSetData)

    // Split the row into an array
    val headerCells = bufferedSource.next.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")

    headerCells.foreach { cell =>
      try {
        var date = dateFormatter.parseDateTime(cell.replaceAll("\"", ""))
        val dates: util.Map[String, String] =
          new util.HashMap[String, String]()
        dates.put("date", date.toString)
        regionData.add(dates)
      } catch {
        case iae: IllegalArgumentException => {
          /* ignore these */
        }
      }
    }
    dataSetData.setData(regionData)
    return dataSetData
  }

  private def SetupColumnsMetadata(dataSetData: DataSetData) = {
    // strip the headers off the top of the file and reuse column meta-data for each row.
    val cols = new Columns
    cols.setColumnMetadata("date", DataType.DATE, DataRole.TIMESTAMP)
    cols.setColumnMetadata("cost", DataType.NUMERIC, DataRole.TARGET)
    // Add the columns to the DataSet Data
    dataSetData.setColumns(cols)
  }

  private def plotTimeSeries(dataSetData: DataSetData, results: SessionResult) = {
    val historical = new TimeSeries("Historical Monthly Home Price")
    val predictions = new TimeSeries("Predicted Monthly Home Price")
    val xySetToPlot = new TimeSeriesCollection

    // Plot Dataset
    dataSetData.getData.forEach { item =>
      val period = RegularTimePeriod.createInstance(
        classOf[Day],
        DateTime.parse(item.get("date")).toDate,
        DateTimeZone.forID("UTC").toTimeZone
      )

      // Add data point to the chart
      var cost = Try(Integer.parseInt(item.get("cost"))).getOrElse(0)
      historical.add(period, cost)
    }

    // Plot Results
    // Plot Dataset
    results.getData.forEach { item =>
      val period = RegularTimePeriod.createInstance(
        classOf[Day],
        DateTime.parse(item.get("date")).toDate,
        DateTimeZone.forID("UTC").toTimeZone
      )

      // Add data point to the chart
      var cost = Try((item.get("cost").toDouble)).getOrElse(0)
      predictions.add(period, cost.asInstanceOf[Number].intValue())
    }
    xySetToPlot.addSeries(historical)
    xySetToPlot.addSeries(predictions)

    val jfreechart = ChartFactory.createTimeSeriesChart(
      dataSetData.getDataSetName,
      "Year",
      "House Cost",
      xySetToPlot,
      true,
      true,
      false
    );

    val xyplot = jfreechart.getPlot.asInstanceOf[XYPlot]
    val dateaxis = xyplot.getDomainAxis.asInstanceOf[DateAxis]
    dateaxis.setTickUnit(
      new DateTickUnit(
        DateTickUnitType.YEAR, 1, new SimpleDateFormat("yyyy")
      ))
    dateaxis.setVerticalTickLabels(true)

    try
      ChartUtilities.saveChartAsJPEG(
        new File(path + "/" + dataSetData.getDataSetName + ".jpeg"),
        jfreechart,
        1000,
        700
      )
    catch {
      case e: Exception =>
        println(e.toString)
    }
  }

  private def plotChart(dataSetData: DataSetData) = {
    val line_chart_dataset = new DefaultCategoryDataset

    dataSetData.getData.forEach {  item =>
      line_chart_dataset.addValue(item.get("cost").toInt, "cost", DateTime.parse(item.get("date")))
    }

    val lineChartObject = ChartFactory.createLineChart(
      "cost over time",
      "Month/Year",
      "Cost",
      line_chart_dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val width = 640
    /* Width of the image */
    val height = 480
    /* Height of the image */
    val lineChart = new File(path + "/" + dataSetData.getDataSetName + ".jpeg")
    ChartUtilities.saveChartAsJPEG(lineChart, lineChartObject, width, height)
  }

  // Utility method
  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try { f(closeable) } finally { closeable.close }

}
