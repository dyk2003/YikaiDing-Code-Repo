//Dataset 1
var dt = spark.read.option("header", "true").csv("attendence_cleaned.csv")
//Dataset 2
var df = spark.read.option("header", "true").csv("snapshot_cleaned.csv")

//Mean & Standard Deviation
//Dataset 1
dt.describe(s"DaysAbsent").show
dt.describe(s"DaysPresent").show
dt.describe(s"Attendance").show
dt.describe(s"ChronicallyAbsentNum").show
dt.describe(s"ChronicallyAbsentPerct").show

//Dataset 2
df.describe(s"EnglishLearner").show
df.describe(s"Male").show
df.describe(s"Female").show
df.describe(s"Asian").show
df.describe(s"Black").show
df.describe(s"Poverty").show

//Cast numertic columns with String type to Int type
//Dataset 1
dt.printSchema()
dt = dt.withColumn("DaysAbsent", col("DaysAbsent").cast("int"))
dt = dt.withColumn("DaysPresent", col("DaysPresent").cast("int"))
dt = dt.withColumn("Attendance", col("Attendance").cast("int"))
dt = dt.withColumn("ChronicallyAbsentNum", col("ChronicallyAbsentNum").cast("int"))
dt = dt.withColumn("ChronicallyAbsentPerct", col("ChronicallyAbsentPerct").cast("int"))
dt.printSchema()

//Dataset 2
df.printSchema()
df = df.withColumn("EnglishLearner", col("EnglishLearner").cast("double"))
df = df.withColumn("Male", col("Male").cast("double"))
df = df.withColumn("Female", col("Female").cast("double"))
df = df.withColumn("Asian", col("Asian").cast("double"))
df = df.withColumn("Black", col("Black").cast("double"))
df = df.withColumn("Poverty", col("Poverty").cast("double"))
df.printSchema()

//Median
//Dataset 1
var median = dt.stat.approxQuantile("DaysAbsent", Array(0.5), 0.01)(0)
median = dt.stat.approxQuantile("DaysPresent", Array(0.5), 0.01)(0)
median = dt.stat.approxQuantile("Attendance", Array(0.5), 0.01)(0)
median = dt.stat.approxQuantile("ChronicallyAbsentNum", Array(0.5), 0.01)(0)
median = dt.stat.approxQuantile("ChronicallyAbsentPerct", Array(0.5), 0.01)(0)

//Dataset 2
var medianTotalEnrollment = df.stat.approxQuantile("EnglishLearner", Array(0.5), 0.01)(0)
var medianMale = df.stat.approxQuantile("Male", Array(0.5), 0.01)(0)
var medianFemale = df.stat.approxQuantile("Female", Array(0.5), 0.01)(0)
var medianAsian = df.stat.approxQuantile("Asian", Array(0.5), 0.01)(0)
var medianBlack = df.stat.approxQuantile("Black", Array(0.5), 0.01)(0)
var medianPoverty = df.stat.approxQuantile("Poverty", Array(0.5), 0.01)(0)

//Mode
//Dataset 1
var modeDf = dt.groupBy("DaysAbsent").count().orderBy(desc("count")).limit(1).show()
modeDf = dt.groupBy("DaysPresent").count().orderBy(desc("count")).limit(1).show()
modeDf = dt.groupBy("Attendance").count().orderBy(desc("count")).limit(1).show()
modeDf = dt.groupBy("ChronicallyAbsentNum").count().orderBy(desc("count")).limit(1).show()
modeDf = dt.groupBy("ChronicallyAbsentPerct").count().orderBy(desc("count")).limit(1).show()

//Dataset 2
val modeTotalEnrollment = df.groupBy("EnglishLearner").count().orderBy(desc("count")).first()(0)
val modeMale = df.groupBy("Male").count().orderBy(desc("count")).first()(0)
val modeFemale = df.groupBy("Female").count().orderBy(desc("count")).first()(0)
val modeAsian = df.groupBy("Asian").count().orderBy(desc("count")).first()(0)
val modeBlack = df.groupBy("Black").count().orderBy(desc("count")).first()(0)
val modePoverty = df.groupBy("Poverty").count().orderBy(desc("count")).first()(0)

// 1. Binary Column: Create a binary column thats shows if a student is from a family in poverty, according to a certain threshold
val povertyThreshold = 77.38 // set poverty threshold
// add a binary column to represent is in poverty or not
// 1: poverty，0: not poverty
val withPoor = df.withColumn("IsPoor", when(col("Poverty") < povertyThreshold, 1).otherwise(0))
withPoor.show()

// 2. add a binary column to represent which race is dominated in the school
df = df.withColumn("dominantRace", when(col("Asian") >= col("Black") && col("Asian") >= col("Hispanic") && col("Asian") >= col("MultiRacial") && col("Asian") >= col("NativeAmerican") && col("Asian") >= col("White"), "Asian").when(col("Black") >= col("Asian") && col("Black") >= col("Hispanic") && col("Black") >= col("MultiRacial") && col("Black") >= col("NativeAmerican") && col("Black") >= col("White"), "Black").when(col("Hispanic") >= col("Black") && col("Hispanic") >= col("Asian") && col("Hispanic") >= col("MultiRacial") && col("Hispanic") >= col("NativeAmerican") && col("Hispanic") >= col("White"), "Hispanic").when(col("MultiRacial") >= col("Asian") && col("MultiRacial") >= col("Hispanic") && col("MultiRacial") >= col("Black") && col("MultiRacial") >= col("NativeAmerican") && col("MultiRacial") >= col("White"), "MultiRacial").when(col("NativeAmerican") >= col("Black") && col("NativeAmerican") >= col("Asian") && col("NativeAmerican") >= col("MultiRacial") && col("NativeAmerican") >= col("Hispanic") && col("NativeAmerican") >= col("White"), "NativeAmerican").when(col("White") >= col("Asian") && col("White") >= col("Hispanic") && col("White") >= col("MultiRacial") && col("White") >= col("NativeAmerican") && col("White") >= col("Black"), "White").otherwise("None"))
// Count the number of schools where each race is dominant based on the newly created column
val raceDominanceCount = withDominantRace.groupBy("dominantRace").agg(count("dominantRace").alias("count"))
raceDominanceCount.show()
