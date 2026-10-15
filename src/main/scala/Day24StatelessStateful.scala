
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds

object Day24StatelessStateful {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day24-Stateless-Stateful")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for updateStateByKey
    ssc.checkpoint("/tmp/day24-bank-checkpoint")

    // Read transactions from localhost port 9999.
    // Each input line represents one transaction.
    // Example: A101
    val transactions = ssc.socketTextStream(
      "localhost",
      9999
    )

    // -----------------------------------------
    // PART 1: STATELESS PROCESSING
    // -----------------------------------------

    val transactionPairs = transactions
      .filter(_.trim.nonEmpty)
      .map(account => (account.trim, 1))

    // Counts only transactions in the current batch.
    val currentBatchCounts = transactionPairs.reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>
      println("\n========== STATELESS: CURRENT BATCH ==========")

      val results = rdd.collect().sortBy(_._1)

      if (results.isEmpty) {
        println("No transactions in this batch.")
      } else {
        results.foreach { case (account, count) =>
          println(s"Account: $account | Current batch count: $count")
        }
      }
    }

    // -----------------------------------------
    // PART 2: STATEFUL PROCESSING
    // -----------------------------------------

    val runningCounts = transactionPairs.updateStateByKey[Int](
      (newCounts: Seq[Int], previousState: Option[Int]) => {
        val newTotal = newCounts.sum + previousState.getOrElse(0)
        Some(newTotal)
      }
    )

    runningCounts.foreachRDD { rdd =>
      println("\n========== STATEFUL: RUNNING TOTAL ==========")

      val results = rdd.collect().sortBy(_._1)

      if (results.isEmpty) {
        println("No account state to display.")
      } else {
        results.foreach { case (account, total) =>
          println(s"Account: $account | Accumulated transactions: $total")
        }
      }
    }

    ssc.start()

    println("\nDay 24 streaming started.")
    println("Send bank account IDs through port 9999.")
    println("Each line represents one transaction.")
    println("Batch interval: 5 seconds.")

    ssc.awaitTermination()
  }
}
