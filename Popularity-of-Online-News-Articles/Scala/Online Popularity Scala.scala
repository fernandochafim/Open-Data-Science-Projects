
object Cells {
  // Spark SQL
  import org.apache.spark.sql.{SparkSession, Column}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{StructField, IntegerType, DoubleType, StringType}
  
  // Spark ML
  import org.apache.spark.ml.{Pipeline, PipelineModel}
  import org.apache.spark.ml.linalg.{Vectors, DenseVector}
  import org.apache.spark.ml.feature.{StringIndexer, StandardScaler, VectorSlicer, VectorIndexer}
  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor, RandomForestRegressionModel, RandomForestRegressor}
  import org.apache.spark.ml.Transformer
  import org.apache.spark.ml.tuning._
  
  
  val spark = SparkSession
    .builder()
    .appName("Spark Example")
    .getOrCreate()   
  
  // Import the implicits, unlike in core Spark the implicits are defined    
  // on the context.    
  import spark.implicits._

  /* ... new cell ... */

  
  val df = spark.read
    .option("header", true)
    .option("inferschema", true)
    .csv("D:/Kaggle/Online News Popularity/OnlineNewsPopularity.csv")

  /* ... new cell ... */

  df

  /* ... new cell ... */

  df.dtypes

  /* ... new cell ... */

  df.select("abs_totle_sentiment_polarity")

  /* ... new cell ... */

  var newDf = df
    for(col <- df.columns){
      newDf = newDf.withColumnRenamed(col, col.replaceAll("\\s", ""))
    }
  
  val fields = newDf.schema.fields.collect {case StructField(name, DoubleType, _, _) => name }
  val correlations = newDf.select(fields.map(corr(_, "shares")): _*).first.toSeq
  
  fields zip correlations

  /* ... new cell ... */

  val labelField = "shares"
  
  val scalarFields: Seq[String] = newDf.schema.fields.collect
    { case StructField(name, DoubleType, _, _) if name != labelField && name != "url" => name }
  
  val scalarData = newDf.map { row => (
    Vectors.dense(scalarFields.map(name =>  row.getAs[Double](name)).toArray)
    , row.getAs[Double](labelField)) }.toDF("features", "label")
  
  scalarData.show

  /* ... new cell ... */

  // Transform Mean and Standard Deviation
  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("features_scaled")
    .setWithStd(true)
    .setWithMean(true)
  
  // Compute summary statistics by fitting the StandardScaler.
  val data_scaled = scaler.fit(scalarData).transform(scalarData)
  data_scaled.show

  /* ... new cell ... */

  val dataReady = data_scaled.select("features_scaled", "label").toDF("features", "label")
  dataReady.show

  /* ... new cell ... */

  val Array(trainData, testData) = dataReady.randomSplit(Array(0.7, 0.3))
  
  def evaluate(ds: DataFrame, model: Transformer): (DataFrame, Double) = {
    val evaluator = new RegressionEvaluator()
     .setLabelCol("label")
     .setPredictionCol("prediction")
     .setMetricName("r2")
    val predictions = model.transform(ds)
    val r2 = evaluator.evaluate(predictions)
    (predictions, r2)
  }
  // 

  /* ... new cell ... */

  val lr = new LinearRegression()
    .setMaxIter(1000)
    .setRegParam(0.3)
  
  //Fit the model 
  val lrModel = lr.fit(trainData)
  
  //Evaluation 
  val (trainLrPredictions, trainLrR2) = evaluate(trainData, lrModel)
  val (testLrPredictions, testLrR2) = evaluate(testData, lrModel)
  
  // Output 
  
  println(s"r2 on train data: $trainLrR2")
  println(s"r2 on test data: $testLrR2")
  
  testLrPredictions.select($"prediction", $"label").show(10)

  /* ... new cell ... */

  val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxBins(1000)
    .setMaxDepth(10)
  
  // Fit the model
  val dtModel = dt.fit(trainData)
  
  //Evaluation 
  val (trainDtPredictions, trainDtR2) = evaluate(trainData, dtModel)
  val (testDtPredictions, testDtR2) = evaluate(testData, dtModel)
  
  // Output 
  
  println(s"r2 on train data: $trainDtR2")
  println(s"r2 on test data: $testDtR2")
  
  testLrPredictions.select($"prediction", $"label").show(10)

  /* ... new cell ... */

  // Train a RandomForest model.
  val rf = new RandomForestRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
  
  // Chain indexer and forest in a Pipeline.
  val pipeline = new Pipeline()
    .setStages(Array(rf))
  
  // Train model. This also runs the indexer.
  val RFmodel = pipeline.fit(trainData)
  
  // Make predictions.
  val predictions = RFmodel.transform(testData)
  
  // Select example rows to display.
  predictions.select("prediction", "label", "features").show(5)
  
  // Select (prediction, true label) and compute test error.
  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)

  /* ... new cell ... */

  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
}
                  