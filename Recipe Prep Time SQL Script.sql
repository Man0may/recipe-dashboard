SELECT
  CASE
    WHEN total_time_minutes <= 30 THEN 'Under 30 min'
    WHEN total_time_minutes <= 60 THEN '30-60 min'
    WHEN total_time_minutes <= 120 THEN '1-2 hours'
    ELSE 'Over 2 hours'
  END AS prep_time_bucket,
  COUNT(*) AS recipe_count
FROM tastydatacatalog.recipes
WHERE total_time_minutes IS NOT NULL
GROUP BY 1
ORDER BY recipe_count DESC;