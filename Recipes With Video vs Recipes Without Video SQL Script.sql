SELECT has_video, COUNT(*) AS recipe_count
FROM tastydatacatalog.recipes
GROUP BY has_video;