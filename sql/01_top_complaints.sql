
SELECT
    complaint_type,
    COUNT(*) AS request_count
FROM nyc_311_requests
GROUP BY complaint_type
ORDER BY request_count DESC
LIMIT 10;
