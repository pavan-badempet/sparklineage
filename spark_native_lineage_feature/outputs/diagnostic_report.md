# ⚡ sparklineage Plan Diagnostic Report

## 📊 Metrics
- **Plan Depth:** 9 stages
- **Shuffle Stages:** 0
- **Joins Analyzed:** 2
- **Total Issues:** 1

## 🚨 Pre-Flight Linter Findings
### `CRITICAL` Cartesian / Cross Join Detected
- **Issue:** Node 'Join' performs a Cartesian cross-join (Join Cross).
- **Recommendation:** Add an explicit join predicate or verify if an intentional cross-join is necessary. Unintended cross joins cause an O(N*M) stage explosion.

## 🌱 Column Lineage
| Column | Upstream Sources | Transformations |
| :--- | :--- | :--- |
| `customer_id` | Range<br>Range<br>Range | (id#3L % 100) |
| `order_id` | Range<br>Range<br>Range | id#3L |
| `unit_price` | Range<br>Range<br>Range | ((id#3L % 40) + 15) |
| `quantity` | Range<br>Range<br>Range | ((id#3L % 4) + 1) |
| `segment` | Range<br>Range<br>Range | CASE WHEN ((id#0L % 5) = 0) THEN Enterprise ELSE Retail END |
| `gross_amount` | Range<br>Range<br>Range | (unit_price#6L * quantity#7L)<br>((id#3L % 4) + 1)<br>((id#3L % 40) + 15) |
| `tax_amount` | Range<br>Range<br>Range | (cast(gross_amount#11L as double) * 0.08)<br>(unit_price#6L * quantity#7L)<br>((id#3L % 4) + 1)<br>((id#3L % 40) + 15) |
| `promo_id` | Range<br>Range<br>Range | id#8L |
| `discount_pct` | Range<br>Range<br>Range | 0.15 |
| `discount_amount` | Range<br>Range<br>Range | (cast(gross_amount#11L as double) * 0.15)<br>(unit_price#6L * quantity#7L)<br>((id#3L % 4) + 1)<br>((id#3L % 40) + 15) |
| `net_revenue` | Range<br>Range<br>Range | ((cast(gross_amount#11L as double) + tax_amount#12) - discount_amount#13)<br>(cast(gross_amount#11L as double) * 0.15)<br>(unit_price#6L * quantity#7L)<br>((id#3L % 4) + 1)<br>((id#3L % 40) + 15)<br>(cast(gross_amount#11L as double) * 0.08) |