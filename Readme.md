## Analysis of Passenger and Flight Data

Given two csv input files, `flightData.csv` and `passengers.csv`, this project answers
five questions and provides answers written to respective csv files:

1. Find the total number of flights for each month
2. Find the top 100 frequent flyers
3. Find the greatest number of countries a passenger visited without being in the UK
4. Find passengers who have been on more than 3 flights together
5. Find passengers who have been on more than N flights together in a designated date range

This project utilizes Apache Spark to read in the source files and create a master
`PassengerFlight` dataset that is then processed by each function utilizing standard
Scala collections and API functions.

### Project Setup
A new Scala project was created in IntelliJ Community Edition with the following versions:

- Scala plugin version 2023.1.19
- Java Oracle OpenJDK version 1.8.0_371
- sbt version 1.9.7
- Scala version 2.13.12

- The `build.sbt` file added the following library dependencies:

````
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.4.8",
"org.apache.spark" %% "spark-sql" % "2.4.8",
"org.scalatest" %% "scalatest" % "3.2.11" % Test
)
````

### Project Structure and Running

- The source csv data files were copied to the `src/main/resources` directory 
- The `src/main/scala` directory contains three Scala classes:

| Class            | Description                                                                                    |
|------------------|------------------------------------------------------------------------------------------------|
| Main             | Main file of project which reads the source csv files and executes functions for each question |
| MainWithSparkSQL | Alternate solutions to Main that utilize the Spark SQL function utilities                      |
| Cases            | Case classes which contain typed structures used in the Main functions                         |
| Utilities        | Utility functions used by functions in Main and MainWithSparkSQL                               |

- The `src/test/scala` directory contains a starter test file for the `Utilities` class

#### How to Run

- Run `src.main.scala.Main` to execute the program and produce output files
- Run `src.main.scala.MainWithSparkSQL` to execute the Spark SQL function solutions
- Run `src.test.scala.UtilitiesTest` to execute the starter unit tests

#### Output

After running `Main`, csv output files are generated in the `output` directory. 
After running `MainWithSparkSQL`, csv output files are generated in the `spark-output` directory.

Each result is written to a subdirectory, with a corresponding `part-****.csv` file containing results:

| Question | Output Directory        |
|----------|-------------------------|
| 1        | flights-per-month       |
| 2        | top-flyers              |
| 3        | longest-runs            |  
| 4        | flights-together        |
| 5 ex 1   | flights-together-date-1 |
| 5 ex 2   | flights-together-date-2 |


