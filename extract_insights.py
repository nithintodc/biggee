#!/usr/bin/env python3
"""
Extract insights from the store-wise analysis Excel file
"""

import pandas as pd
import numpy as np

def extract_insights_from_excel():
    """Extract insights from the store-wise analysis Excel file."""
    
    # Read the Excel file
    excel_file = "Store_Wise_Analysis_20250930_113919.xlsx"
    
    # Read different sheets
    growth_metrics = pd.read_excel(excel_file, sheet_name='Growth_Metrics')
    insights = pd.read_excel(excel_file, sheet_name='Store_Insights')
    summary = pd.read_excel(excel_file, sheet_name='Summary_Statistics')
    top_performers = pd.read_excel(excel_file, sheet_name='Top_10_Performers')
    bottom_performers = pd.read_excel(excel_file, sheet_name='Bottom_10_Performers')
    
    return {
        'growth_metrics': growth_metrics,
        'insights': insights,
        'summary': summary,
        'top_performers': top_performers,
        'bottom_performers': bottom_performers
    }

def create_insights_markdown(data):
    """Create comprehensive insights markdown file."""
    
    growth_metrics = data['growth_metrics']
    insights = data['insights']
    summary = data['summary']
    top_performers = data['top_performers']
    bottom_performers = data['bottom_performers']
    
    markdown_content = f"""# Store-wise Analysis Insights and Recommendations

## Executive Summary

This comprehensive analysis examines store performance across the TODC implementation period, comparing pre-TODC (May 9 - July 8, 2025) and post-TODC (July 9 - September 8, 2025) periods. The analysis covers {len(growth_metrics)} stores and provides detailed insights on sales performance, marketing effectiveness, and growth opportunities.

### Key Performance Indicators

"""
    
    # Add summary statistics
    for _, row in summary.iterrows():
        markdown_content += f"- **{row['Metric']}**: {row['Value']:,.0f} {row['Unit']}\n"
    
    markdown_content += f"""

## Overall Performance Distribution

"""
    
    # Performance distribution
    performance_counts = insights['Overall_Performance'].value_counts()
    for performance, count in performance_counts.items():
        percentage = (count / len(insights)) * 100
        markdown_content += f"- **{performance}**: {count} stores ({percentage:.1f}%)\n"
    
    markdown_content += f"""

## Top Performing Stores

The following stores demonstrated exceptional performance post-TODC implementation:

"""
    
    for i, (_, store) in enumerate(top_performers.iterrows(), 1):
        markdown_content += f"""
### {i}. Store {store['Store_ID']} - {store['Store_Name']}
- **Sales Growth**: {store['Overall_Sales_Growth_Percent']:.1f}%
- **Marketing Sales Growth**: {store['Marketing_Driven_Sales_Growth_Percent']:.1f}%
- **Marketing ROI Change**: {store['Marketing_ROI_Delta']:.1f}%

"""
    
    markdown_content += f"""

## Stores Requiring Immediate Attention

The following stores showed concerning performance trends and require urgent intervention:

"""
    
    for i, (_, store) in enumerate(bottom_performers.iterrows(), 1):
        markdown_content += f"""
### {i}. Store {store['Store_ID']} - {store['Store_Name']}
- **Sales Growth**: {store['Overall_Sales_Growth_Percent']:.1f}%
- **Marketing Sales Growth**: {store['Marketing_Driven_Sales_Growth_Percent']:.1f}%
- **Marketing ROI Change**: {store['Marketing_ROI_Delta']:.1f}%

"""
    
    markdown_content += f"""

## Detailed Store Analysis and Recommendations

### High Priority Stores (Requiring Immediate Action)

"""
    
    high_priority_stores = insights[insights['Priority_Level'] == 'High']
    for _, store in high_priority_stores.iterrows():
        markdown_content += f"""
#### Store {store['Store_ID']} - {store['Store_Name']}
**Performance Rating**: {store['Overall_Performance']}

**Key Insights**:
"""
        for insight in eval(store['Key_Insights']):
            markdown_content += f"- {insight}\n"
        
        markdown_content += f"""
**Recommendations**:
"""
        for rec in eval(store['Recommendations']):
            markdown_content += f"- {rec}\n"
        
        markdown_content += "\n"
    
    markdown_content += f"""

### Medium Priority Stores

"""
    
    medium_priority_stores = insights[insights['Priority_Level'] == 'Medium']
    for _, store in medium_priority_stores.iterrows():
        markdown_content += f"""
#### Store {store['Store_ID']} - {store['Store_Name']}
**Performance Rating**: {store['Overall_Performance']}

**Key Insights**:
"""
        for insight in eval(store['Key_Insights']):
            markdown_content += f"- {insight}\n"
        
        markdown_content += f"""
**Recommendations**:
"""
        for rec in eval(store['Recommendations']):
            markdown_content += f"- {rec}\n"
        
        markdown_content += "\n"
    
    markdown_content += f"""

## Strategic Recommendations by Performance Category

### For Excellent Performing Stores
- **Continue Current Strategies**: These stores are performing exceptionally well. Maintain current operational and marketing strategies.
- **Scale Successful Initiatives**: Identify and replicate successful strategies across other stores.
- **Premium Positioning**: Consider premium menu items or services to further increase AOV.
- **Customer Retention Focus**: Implement loyalty programs to maintain high performance.

### For Good Performing Stores
- **Optimize Marketing Spend**: Focus on high-ROI marketing channels and campaigns.
- **Expand Successful Campaigns**: Scale up marketing campaigns that are driving growth.
- **Operational Efficiency**: Review and optimize operational processes for better scalability.
- **Competitive Analysis**: Study top-performing stores to identify additional growth opportunities.

### For Moderate Performing Stores
- **Marketing Strategy Review**: Conduct comprehensive review of marketing campaigns and channels.
- **Customer Experience Improvement**: Focus on improving customer satisfaction and retention.
- **Pricing Strategy**: Review menu pricing and promotional strategies.
- **Staff Training**: Implement training programs for better customer service and upselling.

### For Poor Performing Stores
- **Urgent Intervention Required**: These stores need immediate attention and intervention.
- **Root Cause Analysis**: Conduct detailed analysis to identify specific issues.
- **Marketing Overhaul**: Complete restructuring of marketing strategies and campaigns.
- **Operational Review**: Comprehensive review of all operational aspects.
- **Competitive Positioning**: Analyze local competition and adjust positioning accordingly.

## Marketing Recommendations

### High-ROI Marketing Strategies
1. **Focus on Self-Serve Campaigns**: Prioritize campaigns with proven ROI metrics.
2. **Customer Acquisition**: Implement targeted customer acquisition campaigns for underperforming stores.
3. **Retention Marketing**: Develop customer retention programs to increase repeat business.
4. **Local Marketing**: Implement location-specific marketing strategies.

### Budget Allocation Guidelines
- **High Performers**: Maintain or slightly increase marketing budget
- **Good Performers**: Optimize existing budget allocation
- **Moderate Performers**: Review and reallocate marketing budget
- **Poor Performers**: Consider temporary budget increase for recovery campaigns

## Operational Recommendations

### Capacity Management
- **High Growth Stores**: Ensure adequate capacity to handle increased order volume
- **Declining Stores**: Review operational efficiency and cost structure

### Menu Optimization
- **High AOV Stores**: Continue premium menu strategies
- **Low AOV Stores**: Implement upselling training and menu optimization

### Technology and Systems
- **All Stores**: Ensure proper integration with TODC systems
- **Underperforming Stores**: Review system utilization and training

## Next Steps

1. **Immediate Actions (Next 30 Days)**:
   - Address high-priority stores with urgent intervention plans
   - Implement quick wins for moderate-performing stores
   - Optimize marketing spend allocation

2. **Short-term Actions (Next 90 Days)**:
   - Complete marketing strategy overhauls for poor-performing stores
   - Implement customer retention programs
   - Conduct comprehensive operational reviews

3. **Long-term Actions (Next 6 Months)**:
   - Scale successful strategies across all stores
   - Implement advanced analytics and reporting
   - Develop predictive models for performance optimization

## Conclusion

The TODC implementation has shown mixed results across stores, with significant opportunities for improvement. By focusing on the specific recommendations for each store category and implementing targeted strategies, we can drive overall performance improvement and maximize the ROI of the TODC investment.

The key to success lies in:
- **Data-driven decision making** based on store-specific performance metrics
- **Targeted interventions** for underperforming stores
- **Scaling successful strategies** from top performers
- **Continuous monitoring and optimization** of all initiatives

---
*Analysis generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Data period: Pre-TODC (2025-05-09 to 2025-07-08) vs Post-TODC (2025-07-09 to 2025-09-08)*
"""
    
    return markdown_content

def main():
    """Main function to extract insights and create markdown file."""
    try:
        # Extract data from Excel
        data = extract_insights_from_excel()
        
        # Create markdown content
        markdown_content = create_insights_markdown(data)
        
        # Write to file
        with open('insights.md', 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print("Insights markdown file created successfully: insights.md")
        
        # Print summary
        print(f"\nSummary:")
        print(f"Total stores analyzed: {len(data['growth_metrics'])}")
        print(f"High priority stores: {len(data['insights'][data['insights']['Priority_Level'] == 'High'])}")
        print(f"Medium priority stores: {len(data['insights'][data['insights']['Priority_Level'] == 'Medium'])}")
        
        performance_counts = data['insights']['Overall_Performance'].value_counts()
        print(f"\nPerformance distribution:")
        for performance, count in performance_counts.items():
            print(f"  {performance}: {count} stores")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
