WITH AggregatedData AS (
    SELECT
        promotion AS PromotionID,
        location_id AS LocationID,
        COUNT(DISTINCT week) AS WeeksCount,
        SUM(sales_in_thousands) AS TotalSales,
        AVG(sales_in_thousands) AS AvgSalesPerWeek
    FROM
        `tc-da-1.turing_data_analytics.wa_marketing_campaign`
    GROUP BY
        promotion, location_id
)
SELECT
    PromotionID,
    COUNT(DISTINCT LocationID) AS NumLocations,
    SUM(TotalSales) AS TotalSalesAllLocations,
    AVG(AvgSalesPerWeek) AS AvgSalesPerLocation,
    STDDEV(AvgSalesPerWeek) AS StdDevSales
FROM
    AggregatedData
GROUP BY
    PromotionID;
