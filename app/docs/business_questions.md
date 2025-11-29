# A. Customer Insights (Customer Analysis)
1. Which cities and states have the highest number of orders?
2. What is the customer repeat purchase rate?(Repeat Rate = (Number of customers with 2+ orders) / (Total unique customers) * 100)
3. How many days does the average customer take between first and second purchase?
4. What is the customer lifetime value (CLTV)?
5. What % of customers churn after the first order?
# B. Order Performance / Operations
6. What is the average delivery time vs estimated time?
7. Which states experience the longest delivery delays?
8. How many orders get delivered late?
9. What is the average number of items per order?
10. What is the cancellation rate?
# C. Seller Insights
11. Which sellers generate the highest revenue?
12. Which sellers have the best/worst customer review scores?
13. How many sellers are active per month?(seller who had atleast one order at that month)
14. Which sellers have the highest return rate?
# D. Product Performance
15. Which product categories generate the most revenue?
16. Which categories have the highest return/refund rate?
17. What categories receive the worst reviews?
18. What is the average price vs freight cost per category?
# E. Payments & Revenue Analysis
19. What is the total revenue per month?
20. What payment methods do customers prefer?
21. What is the average payment value per order?
22. Which months have the highest revenue? (Seasonality)
# F. Marketing / Customer Behavior

23. What is the average order value (AOV)?
24. Which marketing campaigns (based on reviews + repeat behavior) work best?
25. What is the distribution of review scores?
26. How does delivery time impact customer reviews?
# G. Logistics & Geolocation
27. What is the average distance between customer and seller?
28. Does distance affect delivery delay?
29. Which states suffer the highest shipping costs?
30. Which locations have the highest order density?
# H. Advanced Analytics (Great for DE Portfolio)
31. Build an RFM (Recency, Frequency, Monetary) segmentation for customers.
32. Build a weekly revenue forecast.
33. Predict customer review score using ML.
34. Predict order delivery delay using ML.
35. Build a recommendation system using product co-purchases.

# Sales performance over time
How much revenue is generated per month and per year overall and by state?​

Which product categories generate the highest total revenue and quantity sold?​

Which sellers contribute the most revenue in each state or city?​

How does revenue trend over time (monthly/quarterly growth or seasonality)?​

# Customer and region insights
Which states and cities have the highest number of orders and revenue?​

What is the average order value per customer and how does it differ by state or city?​

How many distinct customers ordered in each month, and what is the repeat vs new customer share (using customer_id and order_date_key)?​

# Product and category performance
What are the top 10 products by revenue, quantity sold, and number of orders?​

Which product categories have high order volume but low revenue (cheap products)?​

For each category, what is the average selling price and freight share (freight_value / revenue)?​

# Seller performance and logistics
For each seller, what is total revenue, number of orders, and average items per order?​

Which sellers have the highest average delivery_delay (late deliveries) and delivery_days?​

For each state, what is the average delivery_days and share of orders delivered late (delivery_delay > 0)?​

Which seller–state combinations generate the most revenue but also have high delivery_delay (potential risk)?​

# Order status and fulfillment
What percentage of orders are delivered, shipped, canceled, or unavailable by month and by category?​

How much potential revenue is lost due to canceled or unavailable orders (sum revenue where order_status in ('canceled','unavailable'))?​

How does delivery performance (delivery_days, delay) differ between large vs small orders (bucketed by revenue or count)?​





