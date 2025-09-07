SELECT name, total_time_minutes, original_video_url
FROM tastydatacatalog.recipes
WHERE is_long_prep = true AND has_video = true
ORDER BY total_time_minutes DESC
LIMIT 15;
