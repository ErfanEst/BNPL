package cassandra

object test {

  def selectWriter(featreSet: String): Unit = {

    featreSet match {
      case "BankInfo" =>
        writeBankInfo()
    }

  }

  private def writeBankInfo(): Unit = {

  }

}
