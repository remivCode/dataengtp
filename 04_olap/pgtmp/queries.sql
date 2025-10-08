-- Daily and Monthly Sales by Store
SELECT f.StoreKey, dd.Month, dd.Day, SUM(f.Quantity) AS Quantity, SUM(f.SalesAmount) AS SalesAmount, COUNT(f.SaleID) AS SalesCount
FROM FactSales f
JOIN DimDate dd ON dd.DateKey = f.DateKey
GROUP BY ROLLUP ( dd.Day ), f.StoreKey, dd.Month

-- Sales by Product Category
SELECT dp.Category, SUM(f.Quantity) AS Quantity, SUM(f.SalesAmount) AS SalesAmount, COUNT(f.SaleID) AS SalesCount
FROM FactSales f
JOIN DimProduct dp ON dp.ProductKey = f.ProductKey
GROUP BY dp.Category

-- Top-Selling Products and Suppliers
SELECT ProductKey, COUNT(SaleID) AS SalesNb
FROM FactSales
GROUP BY ProductKey
ORDER BY SalesNb DESC;

SELECT SupplierKey, COUNT(SaleID) AS SalesNb
FROM FactSales
GROUP BY SupplierKey
ORDER BY SalesNb DESC;

-- Average Basket Size (Number of Products per Purchase)
SELECT AVG(SalesNb) AS SalesNb
FROM (
	SELECT COUNT(SaleID)AS SalesNb
	FROM FactSales
	GROUP BY PaymentKey
)