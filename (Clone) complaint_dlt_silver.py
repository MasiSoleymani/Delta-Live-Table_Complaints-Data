import dlt
from pyspark.sql.functions import col, to_date, coalesce

@dlt.table(
    name="complaints_silver_clean",
    comment="Typed & standardized complaints data with expectations."
)
@dlt.expect_or_drop("Month_Year_not_null", "Month_Year IS NOT NULL")
def silver():
    s = dlt.read_stream("complaints_bronze_raw")

    month_year = coalesce(
        to_date(col("Month_Year"), "yyyy-MM"),
        to_date(col("Month_Year"), "MMM yyyy"),
        to_date(col("Month_Year"), "MMM-yyyy"),
        to_date(col("Month_Year"), "yyyy/MM"),
        to_date(col("Month_Year"))
    ).alias("Month_Year")

    return (s.select(
        month_year,
        col("Nature_of_Complaints"),
        col("Complaint_Category"),
        col("Number_of_Complaints").cast("int").alias("Number_of_Complaints"),
        col("Resolution_Time_AvgDay").cast("double").alias("Resolution_Time_AvgDay"),
        col("First_Contact_Resolution_Rate").cast("double").alias("First_Contact_Resolution_Rate"),
        col("Customer_Satisfaction_Score").cast("double").alias("Customer_Satisfaction_Score"),
        col("Open_Complaints").cast("int").alias("Open_Complaints"),
        col("Escalation_Rate").cast("double").alias("Escalation_Rate"),
        col("_source_file"),
        col("_ingested_at"),
        col("_rescued_data")
    ))