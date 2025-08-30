-- GOLD: Resolution KPIs (cleaned)
CREATE LIVE TABLE complaints_gold_resolution_kpis
COMMENT "Monthly KPIs with counts and averages for dashboard analysis."
AS
SELECT
  Month_Year,
  SUM(Number_of_Complaints)              AS Total_Complaints,
  SUM(Open_Complaints)                   AS Open_Complaints,

  -- Resolution time in whole days
  ROUND(AVG(Resolution_Time_AvgDay), 0)  AS Avg_Resolution_Days,

  -- Customer satisfaction with 2 decimals
  ROUND(AVG(Customer_Satisfaction_Score), 2) AS Avg_Customer_Satisfaction,

  -- Escalation rate as a plain number (2 decimals)
  ROUND(AVG(Escalation_Rate), 2)         AS Avg_Escalation_Rate
FROM LIVE.complaints_silver_clean
GROUP BY Month_Year
ORDER BY Month_Year;