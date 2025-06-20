# -*- coding: utf-8 -*-
"""pyspark_course_tracker.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Me1xqIrinfjjRnpwA-7DHRsEi7LHm1gN
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .appName("CourseTrackerAnalysis") \
    .getOrCreate()

progress_df = spark.read.csv("progress (1).csv", header=True, inferSchema=True)
enrollments_df = spark.read.csv("enrollments.csv", header=True, inferSchema=True)

joined_df = enrollments_df.join(
    progress_df,
    on=["student_id", "course_id"],
    how="inner"
)

avg_progress = joined_df.groupBy("course_id").agg(avg("completion").alias("avg_completion"))


enrollment_count = enrollments_df.groupBy("course_id").agg(count("student_id").alias("total_enrollments"))
print("📊 Average Progress by Course:")
avg_progress.show()
print("📈 Total Enrollments by Course:")
enrollment_count.show()
avg_progress.write.csv("avg_progress_report.csv", header=True, mode="overwrite")
enrollment_count.write.csv("enrollment_summary.csv", header=True, mode="overwrite")
spark.stop()