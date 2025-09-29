# TODC Impact Analysis

## Overview

This analysis evaluates the impact of TODC (online restaurant management company) on DoorDash client accounts by comparing performance metrics before and after TODC implementation.

## Analysis Periods

- **Pre-TODC Period**: May 9, 2025 - July 8, 2025
- **Post-TODC Period**: July 9, 2025 - September 8, 2025
- **Year-over-Year Comparison**: 2024 vs 2025 data for the same period

## Data Sources

The analysis uses the following CSV files:

### Financial Data
- `FINANCIAL_DETAILED_TRANSACTIONS_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z.csv`
- `FINANCIAL_DETAILED_TRANSACTIONS_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z.csv`

**Key Metrics**: Subtotal, Commission, Marketing Fees, Net Payout, Customer Discounts

### Marketing Data
- `MARKETING_PROMOTION_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z.csv`
- `MARKETING_PROMOTION_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z.csv`

**Key Metrics**: Orders, Sales, ROAS, Customer Acquisition, Campaign Costs

### Sales Data
- `SALES_viewByTime_byStore_2024-05-09_2024-09-08_xCcwI_2025-09-27T17-00-52Z.csv`
- `SALES_viewByTime_byStore_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z.csv`

**Key Metrics**: Gross Sales, Total Orders, AOV (Average Order Value), Commission

## Analysis Components

### 1. Financial Analysis
- **Order Volume**: Total number of orders processed
- **Revenue Metrics**: Subtotal, net payout, commission rates
- **Cost Analysis**: Marketing fees, customer discounts
- **Growth Metrics**: Pre vs Post TODC performance comparison

### 2. Marketing Campaign Analysis
- **Campaign Performance**: Individual campaign ROI and effectiveness
- **Customer Acquisition**: New vs existing customer metrics
- **ROAS (Return on Ad Spend)**: Marketing investment efficiency
- **Budget Optimization**: Cost per acquisition and campaign efficiency

### 3. Store Performance Analysis
- **Store-level Metrics**: Individual store performance comparison
- **Geographic Performance**: Store location-based analysis
- **Growth Patterns**: Top and bottom performing stores
- **Operational Efficiency**: AOV and order volume trends

### 4. Year-over-Year Analysis
- **Pre-TODC Period YoY**: 2024-05-09 to 2024-07-08 vs 2025-05-09 to 2025-07-08
- **Post-TODC Period YoY**: 2024-07-09 to 2024-09-08 vs 2025-07-09 to 2025-09-08
- **Overall Period YoY**: 2024-05-09 to 2024-09-08 vs 2025-05-09 to 2025-09-08
- **Growth Trends**: Detailed comparison across all periods
- **TODC Impact Assessment**: Pre vs Post TODC YoY performance comparison

## Key Performance Indicators (KPIs)

### Primary KPIs
1. **Total Orders**: Volume of orders processed
2. **Gross Sales**: Total revenue generated
3. **Net Payout**: Revenue after all fees and commissions
4. **Average Order Value (AOV)**: Revenue per order
5. **Marketing ROI**: Return on marketing investment
6. **Customer Acquisition Cost**: Cost to acquire new customers

### Secondary KPIs
1. **Commission Rate**: Percentage of revenue paid as commission
2. **Marketing Spend**: Total marketing investment
3. **Customer Retention**: Existing vs new customer ratio
4. **Store Performance**: Individual location metrics
5. **Campaign Efficiency**: ROAS by campaign type

### Delta Calculations
For every metric, the analysis provides:
- **Delta**: Absolute change (Post - Pre)
- **Delta %**: Percentage change ((Post - Pre) / Pre × 100)
- **Year-over-Year Growth**: Growth compared to previous year

### Visualizations
1. **Line Graphs**: Daily trends showing pre/post TODC performance
2. **Scatter Plot**: Campaign budget vs sales with ROI contour lines
3. **Bar Charts**: Store performance comparisons
4. **Financial Metrics**: Side-by-side pre/post comparisons

## ROI Calculations

### Marketing ROI Formula
```
ROI = (Sales Generated - Total Marketing Cost) / Total Marketing Cost × 100
```

### Campaign ROI Analysis
- **Total Cost**: Customer discounts + Marketing fees
- **Revenue Generated**: Sales attributed to campaigns
- **Net Profit**: Revenue - Total Cost
- **ROI Percentage**: Net Profit / Total Cost × 100

## Usage Instructions

### Prerequisites
```bash
pip install pandas numpy matplotlib seaborn openpyxl
```

### Running the Analysis
```bash
python todc_analysis.py
```

### Output
The script generates:
1. **Console Output**: Real-time analysis results and insights with detailed delta calculations
2. **Excel Report**: Comprehensive analysis with multiple sheets:
   - Financial_Analysis (Pre/Post comparison with deltas)
   - Marketing_Campaign_Comparison (Pre/Post campaign analysis)
   - Store_Performance_Comparison (Pre/Post store metrics)
   - Year_Over_Year_Analysis (2024 vs 2025 with growth details)
   - Marketing_ROI_Analysis (ROI metrics breakdown)
   - Insights_Recommendations
3. **Visualizations**: PNG charts saved in 'charts' directory:
   - line_graphs_trends.png (Daily trends for sales, orders, AOV, marketing spend)
   - campaign_budget_vs_sales.png (Scatter plot with ROI contours)
   - store_performance_comparison.png (Store-level comparisons)
   - financial_metrics_comparison.png (Key financial metrics)

## Analysis Methodology

### Data Processing
1. **Date Standardization**: All datasets are processed to use consistent date formats
2. **Period Filtering**: Data is filtered for specific pre/post TODC periods
3. **Metric Calculation**: KPIs are calculated using standardized formulas
4. **Growth Analysis**: Percentage and absolute growth metrics are computed

### Statistical Analysis
- **Comparative Analysis**: Pre vs Post TODC performance
- **Trend Analysis**: Year-over-year growth patterns
- **Correlation Analysis**: Relationship between marketing spend and revenue
- **Performance Ranking**: Store and campaign performance rankings

## Key Insights Generated

### Performance Insights
- Order volume growth/decline patterns
- Revenue optimization opportunities
- Marketing campaign effectiveness
- Store performance variations

### Strategic Recommendations
- Budget allocation optimization
- Campaign scaling recommendations
- Store-specific improvement strategies
- Operational efficiency enhancements

## File Structure

```
├── todc_analysis.py          # Main analysis script
├── README.md                 # This documentation
├── financial_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z/
│   └── FINANCIAL_DETAILED_TRANSACTIONS_*.csv
├── financial_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z/
│   └── FINANCIAL_DETAILED_TRANSACTIONS_*.csv
├── marketing_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z/
│   └── MARKETING_PROMOTION_*.csv
├── marketing_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z/
│   └── MARKETING_PROMOTION_*.csv
├── SALES_viewByTime_2024-05-09_2024-09-08_xCcwI_2025-09-27T17-00-52Z/
│   └── SALES_viewByTime_byStore_*.csv
└── SALES_viewByTime_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z/
    └── SALES_viewByTime_byStore_*.csv
```

## Customization Options

### Date Range Modification
To analyze different periods, modify the date variables in the `TODCAnalyzer` class:
```python
self.pre_todc_start = '2025-05-09'
self.pre_todc_end = '2025-07-08'
self.post_todc_start = '2025-07-09'
self.post_todc_end = '2025-09-08'
```

### Additional Metrics
To add new KPIs, extend the metric calculation functions in the respective analysis methods.

### Visualization
The script can be extended to include matplotlib/seaborn visualizations for better data presentation.

## Troubleshooting

### Common Issues
1. **File Not Found**: Ensure all CSV files are in the correct directories
2. **Date Format Errors**: Check that date columns are properly formatted
3. **Memory Issues**: For large datasets, consider processing in chunks
4. **Missing Dependencies**: Install required Python packages

### Data Validation
The script includes basic data validation:
- Checks for empty datasets
- Validates date ranges
- Handles missing values
- Provides error messages for common issues

## Support

For questions or issues with the analysis:
1. Check the console output for error messages
2. Verify all data files are present and properly formatted
3. Ensure all required Python packages are installed
4. Review the analysis methodology in this README

## Version History

- **v1.0**: Initial release with comprehensive TODC impact analysis
- Includes financial, marketing, store performance, and year-over-year analysis
- Generates Excel reports with multiple analysis sheets
- Provides actionable insights and recommendations
