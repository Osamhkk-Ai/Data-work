# Week 2 Summary — ETL + EDA

## Key findings
- Most orders are low-to-mid value; high amounts are rare after winsorization.
- SA generates the highest total revenue, followed by AE and EG.
- Refund rates are high across countries, with AE slightly higher than others.
- Daily revenue varies significantly with clear peaks during the month.

## Definitions
- Revenue = sum(amount) where status_clean == "paid".
- Refund rate = refunded orders / total orders.
- Amounts were winsorized to reduce outliers.
- Time window = daily aggregation.

## Data quality caveats
- Some missing values and outliers were handled during cleaning.
- Refund rates may be unstable for low-volume countries.

## Next questions
- Why is the refund rate higher in AE?
- What drives daily revenue spikes?