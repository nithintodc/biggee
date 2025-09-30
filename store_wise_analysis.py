#!/usr/bin/env python3
"""
Comprehensive Store-wise Analysis Script
=======================================

This script performs an in-depth analysis of store-wise data including:
- Overall sales, marketing-driven sales, organic sales
- Growth from pre to post TODC period
- Marketing spend efficiency and ROI
- Store-specific insights and recommendations

Author: TODC Analytics Team
Date: 2025-09-29

Analysis Periods:
- Pre-TODC: 2025-05-09 to 2025-07-08
- Post-TODC: 2025-07-09 to 2025-09-08
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
import os
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Create output directory for charts
if not os.path.exists('charts'):
    os.makedirs('charts')

class StoreWiseAnalyzer:
    def __init__(self):
        """Initialize the Store-wise Analyzer with data paths and analysis periods."""
        self.data_paths = {
            'financial_2024': 'financial_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z/FINANCIAL_DETAILED_TRANSACTIONS_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z.csv',
            'financial_2025': 'financial_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z/FINANCIAL_DETAILED_TRANSACTIONS_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z.csv',
            'marketing_2024': 'marketing_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z/MARKETING_PROMOTION_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z.csv',
            'marketing_2025': 'marketing_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z/MARKETING_PROMOTION_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z.csv',
            'marketing_sponsored_2025': 'marketing_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z/MARKETING_SPONSORED_LISTING_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z.csv',
            'sales_2024': 'SALES_viewByTime_2024-05-09_2024-09-08_xCcwI_2025-09-27T17-00-52Z/SALES_viewByTime_byStore_2024-05-09_2024-09-08_xCcwI_2025-09-27T17-00-52Z.csv',
            'sales_2025': 'SALES_viewByTime_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z/SALES_viewByTime_byStore_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z.csv'
        }
        
        # Define analysis periods
        self.pre_todc_start = '2025-05-09'
        self.pre_todc_end = '2025-07-08'
        self.post_todc_start = '2025-07-09'
        self.post_todc_end = '2025-09-08'
        
        # Load all data
        self.load_data()
        
    def load_data(self):
        """Load all CSV files and prepare them for analysis."""
        print("Loading data files for store-wise analysis...")
        
        # Load financial data
        self.financial_2024 = pd.read_csv(self.data_paths['financial_2024'])
        self.financial_2025 = pd.read_csv(self.data_paths['financial_2025'])
        
        # Load marketing data
        self.marketing_2024 = pd.read_csv(self.data_paths['marketing_2024'])
        self.marketing_2025 = pd.read_csv(self.data_paths['marketing_2025'])
        self.marketing_sponsored_2025 = pd.read_csv(self.data_paths['marketing_sponsored_2025'])
        
        # Load sales data
        self.sales_2024 = pd.read_csv(self.data_paths['sales_2024'])
        self.sales_2025 = pd.read_csv(self.data_paths['sales_2025'])
        
        # Process date columns
        self.process_dates()
        
        print("Data loaded successfully!")
        print(f"Financial 2024: {len(self.financial_2024):,} records")
        print(f"Financial 2025: {len(self.financial_2025):,} records")
        print(f"Marketing 2024: {len(self.marketing_2024):,} records")
        print(f"Marketing 2025: {len(self.marketing_2025):,} records")
        print(f"Marketing Sponsored 2025: {len(self.marketing_sponsored_2025):,} records")
        print(f"Sales 2024: {len(self.sales_2024):,} records")
        print(f"Sales 2025: {len(self.sales_2025):,} records")
        
    def process_dates(self):
        """Process and standardize date columns across all datasets."""
        # Financial data - use 'Timestamp UTC date'
        if 'Timestamp UTC date' in self.financial_2024.columns:
            self.financial_2024['date'] = pd.to_datetime(self.financial_2024['Timestamp UTC date'])
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Timestamp UTC date'])
        elif 'Payout date' in self.financial_2024.columns:
            self.financial_2024['date'] = pd.to_datetime(self.financial_2024['Payout date'])
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Payout date'])
        
        # Marketing data - use 'Date'
        self.marketing_2024['date'] = pd.to_datetime(self.marketing_2024['Date'])
        self.marketing_2025['date'] = pd.to_datetime(self.marketing_2025['Date'])
        self.marketing_sponsored_2025['date'] = pd.to_datetime(self.marketing_sponsored_2025['Date'])
        
        # Sales data - use 'Start Date'
        self.sales_2024['date'] = pd.to_datetime(self.sales_2024['Start Date'])
        self.sales_2025['date'] = pd.to_datetime(self.sales_2025['Start Date'])
        
    def filter_by_period(self, df, start_date, end_date, date_col='date'):
        """Filter dataframe by date range."""
        return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    
    def combine_marketing_data(self):
        """Combine promotion and sponsored marketing data for 2025."""
        # Add source column to distinguish between promotion and sponsored
        self.marketing_2025['Source'] = 'Promotion'
        self.marketing_sponsored_2025['Source'] = 'Sponsored'
        
        # Select common columns for combination
        common_cols = ['Date', 'Store ID', 'Store name', 'Currency', 'Orders', 'Sales', 'Average order value']
        promotion_cols = common_cols + ['Customer discounts from marketing | (Funded by you)', 'Source']
        sponsored_cols = common_cols + ['Source']
        
        # For sponsored, we don't have customer discounts, so we'll set it to 0
        self.marketing_sponsored_2025['Customer discounts from marketing | (Funded by you)'] = 0
        
        # Combine the datasets
        marketing_combined = pd.concat([
            self.marketing_2025[promotion_cols],
            self.marketing_sponsored_2025[sponsored_cols + ['Customer discounts from marketing | (Funded by you)']]
        ], ignore_index=True)
        
        # Convert Date to datetime
        marketing_combined['date'] = pd.to_datetime(marketing_combined['Date'])
        
        return marketing_combined
    
    def analyze_store_performance_by_period(self, period_name, start_date, end_date):
        """Analyze store performance for a specific period."""
        print(f"\nAnalyzing {period_name} period ({start_date} to {end_date})...")
        
        # Filter data for the period
        financial_data = self.filter_by_period(self.financial_2025, start_date, end_date)
        sales_data = self.filter_by_period(self.sales_2025, start_date, end_date)
        marketing_data = self.filter_by_period(self.combine_marketing_data(), start_date, end_date)
        
        # Filter only orders from financial data
        orders_data = financial_data[financial_data['Transaction type'] == 'Order']
        
        # Calculate store-wise metrics
        store_metrics = {}
        
        # Get all unique store IDs
        all_store_ids = set()
        if len(orders_data) > 0:
            all_store_ids.update(orders_data['Store ID'].unique())
        if len(sales_data) > 0:
            all_store_ids.update(sales_data['Store ID'].unique())
        if len(marketing_data) > 0:
            all_store_ids.update(marketing_data['Store ID'].unique())
        
        for store_id in all_store_ids:
            store_metrics[store_id] = {
                'Store_ID': store_id,
                'Store_Name': 'Unknown',
                'Period': period_name,
                'Overall_Sales': 0,
                'Total_Orders': 0,
                'Marketing_Driven_Sales': 0,
                'Organic_Sales': 0,
                'Marketing_Spend': 0,
                'Marketing_Orders': 0,
                'Net_Payout': 0,
                'Avg_Order_Value': 0,
                'Marketing_ROI': 0,
                'Marketing_Percentage': 0,
                'Organic_Percentage': 0
            }
            
            # Financial metrics (from orders)
            store_orders = orders_data[orders_data['Store ID'] == store_id]
            if len(store_orders) > 0:
                store_metrics[store_id]['Overall_Sales'] = store_orders['Subtotal'].sum()
                store_metrics[store_id]['Total_Orders'] = len(store_orders)
                store_metrics[store_id]['Net_Payout'] = store_orders['Net total'].sum()
                store_metrics[store_id]['Avg_Order_Value'] = store_orders['Subtotal'].mean()
                
                # Marketing costs
                if 'Marketing fees | (including any applicable taxes)' in store_orders.columns:
                    marketing_fees = store_orders['Marketing fees | (including any applicable taxes)'].sum()
                else:
                    marketing_fees = 0
                
                if 'Customer discounts from marketing | (funded by you)' in store_orders.columns:
                    customer_discounts = store_orders['Customer discounts from marketing | (funded by you)'].sum()
                else:
                    customer_discounts = 0
                
                store_metrics[store_id]['Marketing_Spend'] = marketing_fees + customer_discounts
            
            # Sales metrics (from sales data)
            store_sales = sales_data[sales_data['Store ID'] == store_id]
            if len(store_sales) > 0:
                # Use sales data if it's higher than financial data
                sales_total = store_sales['Gross Sales'].sum()
                if sales_total > store_metrics[store_id]['Overall_Sales']:
                    store_metrics[store_id]['Overall_Sales'] = sales_total
                    store_metrics[store_id]['Total_Orders'] = store_sales['Total Delivered or Picked Up Orders'].sum()
                    store_metrics[store_id]['Avg_Order_Value'] = store_sales['AOV'].mean()
                
                # Get store name
                store_metrics[store_id]['Store_Name'] = store_sales['Store Name'].iloc[0]
            
            # Marketing metrics
            store_marketing = marketing_data[marketing_data['Store ID'] == store_id]
            if len(store_marketing) > 0:
                store_metrics[store_id]['Marketing_Driven_Sales'] = store_marketing['Sales'].sum()
                store_metrics[store_id]['Marketing_Orders'] = store_marketing['Orders'].sum()
                
                # Calculate marketing spend from marketing data
                marketing_spend_mkt = store_marketing['Customer discounts from marketing | (Funded by you)'].sum()
                if marketing_spend_mkt > store_metrics[store_id]['Marketing_Spend']:
                    store_metrics[store_id]['Marketing_Spend'] = marketing_spend_mkt
            
            # Calculate organic sales
            store_metrics[store_id]['Organic_Sales'] = (
                store_metrics[store_id]['Overall_Sales'] - store_metrics[store_id]['Marketing_Driven_Sales']
            )
            
            # Calculate percentages
            if store_metrics[store_id]['Overall_Sales'] > 0:
                store_metrics[store_id]['Marketing_Percentage'] = (
                    store_metrics[store_id]['Marketing_Driven_Sales'] / store_metrics[store_id]['Overall_Sales'] * 100
                )
                store_metrics[store_id]['Organic_Percentage'] = (
                    store_metrics[store_id]['Organic_Sales'] / store_metrics[store_id]['Overall_Sales'] * 100
                )
            
            # Calculate Marketing ROI
            if store_metrics[store_id]['Marketing_Spend'] > 0:
                store_metrics[store_id]['Marketing_ROI'] = (
                    (store_metrics[store_id]['Marketing_Driven_Sales'] - store_metrics[store_id]['Marketing_Spend']) /
                    store_metrics[store_id]['Marketing_Spend'] * 100
                )
        
        return pd.DataFrame.from_dict(store_metrics, orient='index')
    
    def calculate_store_growth_metrics(self, pre_metrics, post_metrics):
        """Calculate growth metrics between pre and post TODC periods."""
        print("\nCalculating store growth metrics...")
        
        # Get all unique store IDs
        all_store_ids = set(pre_metrics['Store_ID']) | set(post_metrics['Store_ID'])
        
        growth_metrics = []
        
        for store_id in all_store_ids:
            # Get pre and post data for this store
            pre_data = pre_metrics[pre_metrics['Store_ID'] == store_id]
            post_data = post_metrics[post_metrics['Store_ID'] == store_id]
            
            growth_data = {
                'Store_ID': store_id,
                'Store_Name': 'Unknown'
            }
            
            # Get store name
            if len(pre_data) > 0:
                growth_data['Store_Name'] = pre_data['Store_Name'].iloc[0]
            elif len(post_data) > 0:
                growth_data['Store_Name'] = post_data['Store_Name'].iloc[0]
            
            # Calculate growth for each metric
            metrics_to_analyze = [
                'Overall_Sales', 'Total_Orders', 'Marketing_Driven_Sales', 
                'Organic_Sales', 'Marketing_Spend', 'Net_Payout', 'Avg_Order_Value'
            ]
            
            for metric in metrics_to_analyze:
                pre_val = pre_data[metric].iloc[0] if len(pre_data) > 0 else 0
                post_val = post_data[metric].iloc[0] if len(post_data) > 0 else 0
                
                # Calculate absolute and percentage growth
                growth_data[f'{metric}_Pre'] = pre_val
                growth_data[f'{metric}_Post'] = post_val
                growth_data[f'{metric}_Delta'] = post_val - pre_val
                growth_data[f'{metric}_Growth_Percent'] = (
                    ((post_val - pre_val) / pre_val * 100) if pre_val > 0 else 0
                )
            
            # Calculate Marketing ROI growth
            pre_roi = pre_data['Marketing_ROI'].iloc[0] if len(pre_data) > 0 else 0
            post_roi = post_data['Marketing_ROI'].iloc[0] if len(post_data) > 0 else 0
            growth_data['Marketing_ROI_Pre'] = pre_roi
            growth_data['Marketing_ROI_Post'] = post_roi
            growth_data['Marketing_ROI_Delta'] = post_roi - pre_roi
            
            # Calculate Marketing Percentage growth
            pre_mkt_pct = pre_data['Marketing_Percentage'].iloc[0] if len(pre_data) > 0 else 0
            post_mkt_pct = post_data['Marketing_Percentage'].iloc[0] if len(post_data) > 0 else 0
            growth_data['Marketing_Percentage_Pre'] = pre_mkt_pct
            growth_data['Marketing_Percentage_Post'] = post_mkt_pct
            growth_data['Marketing_Percentage_Delta'] = post_mkt_pct - pre_mkt_pct
            
            growth_metrics.append(growth_data)
        
        return pd.DataFrame(growth_metrics)
    
    def generate_store_insights(self, growth_metrics):
        """Generate insights and recommendations for each store."""
        print("\nGenerating store-specific insights and recommendations...")
        
        insights = []
        
        for _, store in growth_metrics.iterrows():
            store_insight = {
                'Store_ID': store['Store_ID'],
                'Store_Name': store['Store_Name'],
                'Overall_Performance': 'Unknown',
                'Key_Insights': [],
                'Recommendations': [],
                'Priority_Level': 'Medium'
            }
            
            # Analyze overall sales growth
            sales_growth = store['Overall_Sales_Growth_Percent']
            if sales_growth > 50:
                store_insight['Overall_Performance'] = 'Excellent'
                store_insight['Key_Insights'].append(f"Outstanding sales growth of {sales_growth:.1f}% post-TODC")
                store_insight['Recommendations'].append("Continue current strategies and consider scaling successful initiatives")
            elif sales_growth > 20:
                store_insight['Overall_Performance'] = 'Good'
                store_insight['Key_Insights'].append(f"Strong sales growth of {sales_growth:.1f}% post-TODC")
                store_insight['Recommendations'].append("Optimize and expand successful marketing campaigns")
            elif sales_growth > 0:
                store_insight['Overall_Performance'] = 'Moderate'
                store_insight['Key_Insights'].append(f"Modest sales growth of {sales_growth:.1f}% post-TODC")
                store_insight['Recommendations'].append("Review and improve marketing strategies for better growth")
            else:
                store_insight['Overall_Performance'] = 'Poor'
                store_insight['Key_Insights'].append(f"Sales declined by {abs(sales_growth):.1f}% post-TODC")
                store_insight['Recommendations'].append("Urgent review needed - implement aggressive growth strategies")
                store_insight['Priority_Level'] = 'High'
            
            # Analyze marketing performance
            marketing_growth = store['Marketing_Driven_Sales_Growth_Percent']
            marketing_roi_delta = store['Marketing_ROI_Delta']
            
            if marketing_growth > 30 and marketing_roi_delta > 0:
                store_insight['Key_Insights'].append(f"Marketing campaigns are highly effective with {marketing_growth:.1f}% growth and improved ROI")
                store_insight['Recommendations'].append("Increase marketing budget allocation for this store")
            elif marketing_growth > 0 and marketing_roi_delta > 0:
                store_insight['Key_Insights'].append(f"Marketing is performing well with {marketing_growth:.1f}% growth and positive ROI trend")
                store_insight['Recommendations'].append("Optimize existing campaigns and test new marketing channels")
            elif marketing_growth < 0 or marketing_roi_delta < -10:
                store_insight['Key_Insights'].append("Marketing performance needs improvement")
                store_insight['Recommendations'].append("Review and restructure marketing campaigns - consider different strategies")
                if store_insight['Priority_Level'] != 'High':
                    store_insight['Priority_Level'] = 'High'
            
            # Analyze organic sales
            organic_growth = store['Organic_Sales_Growth_Percent']
            if organic_growth > 20:
                store_insight['Key_Insights'].append(f"Strong organic growth of {organic_growth:.1f}% indicates good brand recognition")
                store_insight['Recommendations'].append("Leverage organic growth by improving customer experience and retention")
            elif organic_growth < -10:
                store_insight['Key_Insights'].append(f"Organic sales declined by {abs(organic_growth):.1f}% - brand awareness may be decreasing")
                store_insight['Recommendations'].append("Focus on brand building and customer retention strategies")
            
            # Analyze order volume
            order_growth = store['Total_Orders_Growth_Percent']
            if order_growth > 30:
                store_insight['Key_Insights'].append(f"Order volume increased significantly by {order_growth:.1f}%")
                store_insight['Recommendations'].append("Ensure operational capacity can handle increased order volume")
            elif order_growth < -15:
                store_insight['Key_Insights'].append(f"Order volume decreased by {abs(order_growth):.1f}% - customer acquisition needs attention")
                store_insight['Recommendations'].append("Implement customer acquisition campaigns and improve visibility")
            
            # Analyze AOV
            aov_growth = store['Avg_Order_Value_Growth_Percent']
            if aov_growth > 10:
                store_insight['Key_Insights'].append(f"Average order value increased by {aov_growth:.1f}% - upselling is effective")
                store_insight['Recommendations'].append("Continue upselling strategies and consider premium menu items")
            elif aov_growth < -5:
                store_insight['Key_Insights'].append(f"Average order value decreased by {abs(aov_growth):.1f}%")
                store_insight['Recommendations'].append("Review menu pricing and implement upselling training")
            
            # Marketing spend analysis
            marketing_spend_growth = store['Marketing_Spend_Growth_Percent']
            if marketing_spend_growth > 50 and marketing_roi_delta < -20:
                store_insight['Key_Insights'].append("Marketing spend increased significantly but ROI decreased")
                store_insight['Recommendations'].append("Optimize marketing spend allocation - focus on high-performing campaigns")
            elif marketing_spend_growth < -20 and marketing_growth > 0:
                store_insight['Key_Insights'].append("Marketing spend decreased but sales still grew - efficient marketing")
                store_insight['Recommendations'].append("Consider increasing marketing budget to accelerate growth")
            
            insights.append(store_insight)
        
        return pd.DataFrame(insights)
    
    def create_store_visualizations(self, growth_metrics, insights):
        """Create visualizations for store analysis."""
        print("\nCreating store analysis visualizations...")
        
        # 1. Store Performance Overview
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        
        # Sales Growth Distribution
        axes[0, 0].hist(growth_metrics['Overall_Sales_Growth_Percent'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].axvline(x=0, color='red', linestyle='--', alpha=0.7)
        axes[0, 0].set_title('Distribution of Sales Growth Across Stores', fontsize=14, fontweight='bold')
        axes[0, 0].set_xlabel('Sales Growth (%)')
        axes[0, 0].set_ylabel('Number of Stores')
        axes[0, 0].grid(True, alpha=0.3)
        
        # Top 10 Stores by Sales Growth
        top_stores = growth_metrics.nlargest(10, 'Overall_Sales_Growth_Percent')
        axes[0, 1].barh(range(len(top_stores)), top_stores['Overall_Sales_Growth_Percent'], color='lightgreen', alpha=0.8)
        axes[0, 1].set_yticks(range(len(top_stores)))
        axes[0, 1].set_yticklabels([f"Store {row['Store_ID']}" for _, row in top_stores.iterrows()], fontsize=8)
        axes[0, 1].set_title('Top 10 Stores by Sales Growth', fontsize=14, fontweight='bold')
        axes[0, 1].set_xlabel('Sales Growth (%)')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Marketing ROI vs Sales Growth
        scatter = axes[1, 0].scatter(growth_metrics['Marketing_ROI_Delta'], growth_metrics['Overall_Sales_Growth_Percent'], 
                                   c=growth_metrics['Marketing_Spend_Growth_Percent'], cmap='viridis', alpha=0.7, s=100)
        axes[1, 0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        axes[1, 0].axvline(x=0, color='red', linestyle='--', alpha=0.5)
        axes[1, 0].set_title('Marketing ROI Change vs Sales Growth', fontsize=14, fontweight='bold')
        axes[1, 0].set_xlabel('Marketing ROI Change (%)')
        axes[1, 0].set_ylabel('Sales Growth (%)')
        axes[1, 0].grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=axes[1, 0], label='Marketing Spend Growth (%)')
        
        # Performance Categories
        performance_counts = insights['Overall_Performance'].value_counts()
        colors = ['green', 'lightgreen', 'orange', 'red']
        axes[1, 1].pie(performance_counts.values, labels=performance_counts.index, autopct='%1.1f%%', 
                      colors=colors[:len(performance_counts)], startangle=90)
        axes[1, 1].set_title('Store Performance Distribution', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('charts/store_performance_overview.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Store performance overview saved: charts/store_performance_overview.png")
        
        # 2. Marketing Analysis
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        
        # Marketing Spend vs Marketing Sales Growth
        axes[0, 0].scatter(growth_metrics['Marketing_Spend_Growth_Percent'], growth_metrics['Marketing_Driven_Sales_Growth_Percent'], 
                          alpha=0.7, s=100, color='purple')
        axes[0, 0].set_title('Marketing Spend Growth vs Marketing Sales Growth', fontsize=14, fontweight='bold')
        axes[0, 0].set_xlabel('Marketing Spend Growth (%)')
        axes[0, 0].set_ylabel('Marketing Sales Growth (%)')
        axes[0, 0].grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(growth_metrics['Marketing_Spend_Growth_Percent'].fillna(0), 
                      growth_metrics['Marketing_Driven_Sales_Growth_Percent'].fillna(0), 1)
        p = np.poly1d(z)
        axes[0, 0].plot(growth_metrics['Marketing_Spend_Growth_Percent'].fillna(0), 
                       p(growth_metrics['Marketing_Spend_Growth_Percent'].fillna(0)), "r--", alpha=0.8)
        
        # Organic vs Marketing Sales Growth
        axes[0, 1].scatter(growth_metrics['Marketing_Driven_Sales_Growth_Percent'], growth_metrics['Organic_Sales_Growth_Percent'], 
                          alpha=0.7, s=100, color='orange')
        axes[0, 1].set_title('Marketing Sales Growth vs Organic Sales Growth', fontsize=14, fontweight='bold')
        axes[0, 1].set_xlabel('Marketing Sales Growth (%)')
        axes[0, 1].set_ylabel('Organic Sales Growth (%)')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Marketing ROI Distribution
        axes[1, 0].hist(growth_metrics['Marketing_ROI_Delta'], bins=20, alpha=0.7, color='lightcoral', edgecolor='black')
        axes[1, 0].axvline(x=0, color='red', linestyle='--', alpha=0.7)
        axes[1, 0].set_title('Distribution of Marketing ROI Changes', fontsize=14, fontweight='bold')
        axes[1, 0].set_xlabel('Marketing ROI Change (%)')
        axes[1, 0].set_ylabel('Number of Stores')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Priority Level Distribution
        priority_counts = insights['Priority_Level'].value_counts()
        colors = ['red', 'orange', 'lightgreen']
        axes[1, 1].pie(priority_counts.values, labels=priority_counts.index, autopct='%1.1f%%', 
                      colors=colors[:len(priority_counts)], startangle=90)
        axes[1, 1].set_title('Store Priority Level Distribution', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('charts/store_marketing_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Store marketing analysis saved: charts/store_marketing_analysis.png")
    
    def export_to_excel(self, pre_metrics, post_metrics, growth_metrics, insights):
        """Export store analysis results to Excel file."""
        print("\nExporting store analysis to Excel...")
        
        filename = f"Store_Wise_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Pre-TODC Metrics
            pre_metrics.to_excel(writer, sheet_name='Pre_TODC_Metrics', index=False)
            
            # Post-TODC Metrics
            post_metrics.to_excel(writer, sheet_name='Post_TODC_Metrics', index=False)
            
            # Growth Metrics
            growth_metrics.to_excel(writer, sheet_name='Growth_Metrics', index=False)
            
            # Store Insights and Recommendations
            insights.to_excel(writer, sheet_name='Store_Insights', index=False)
            
            # Summary Statistics
            summary_data = []
            
            # Overall summary
            summary_data.append({
                'Metric': 'Total Stores Analyzed',
                'Value': len(growth_metrics),
                'Unit': 'Stores'
            })
            
            # Sales growth summary
            avg_sales_growth = growth_metrics['Overall_Sales_Growth_Percent'].mean()
            summary_data.append({
                'Metric': 'Average Sales Growth',
                'Value': avg_sales_growth,
                'Unit': '%'
            })
            
            # Marketing performance summary
            avg_marketing_growth = growth_metrics['Marketing_Driven_Sales_Growth_Percent'].mean()
            summary_data.append({
                'Metric': 'Average Marketing Sales Growth',
                'Value': avg_marketing_growth,
                'Unit': '%'
            })
            
            # Performance distribution
            excellent_stores = len(insights[insights['Overall_Performance'] == 'Excellent'])
            good_stores = len(insights[insights['Overall_Performance'] == 'Good'])
            moderate_stores = len(insights[insights['Overall_Performance'] == 'Moderate'])
            poor_stores = len(insights[insights['Overall_Performance'] == 'Poor'])
            
            summary_data.extend([
                {'Metric': 'Excellent Performing Stores', 'Value': excellent_stores, 'Unit': 'Stores'},
                {'Metric': 'Good Performing Stores', 'Value': good_stores, 'Unit': 'Stores'},
                {'Metric': 'Moderate Performing Stores', 'Value': moderate_stores, 'Unit': 'Stores'},
                {'Metric': 'Poor Performing Stores', 'Value': poor_stores, 'Unit': 'Stores'}
            ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary_Statistics', index=False)
            
            # Top and Bottom Performers
            top_performers = growth_metrics.nlargest(10, 'Overall_Sales_Growth_Percent')[
                ['Store_ID', 'Store_Name', 'Overall_Sales_Growth_Percent', 'Marketing_Driven_Sales_Growth_Percent', 'Marketing_ROI_Delta']
            ]
            top_performers.to_excel(writer, sheet_name='Top_10_Performers', index=False)
            
            bottom_performers = growth_metrics.nsmallest(10, 'Overall_Sales_Growth_Percent')[
                ['Store_ID', 'Store_Name', 'Overall_Sales_Growth_Percent', 'Marketing_Driven_Sales_Growth_Percent', 'Marketing_ROI_Delta']
            ]
            bottom_performers.to_excel(writer, sheet_name='Bottom_10_Performers', index=False)
        
        print(f"Store analysis exported to: {filename}")
        return filename
    
    def run_complete_analysis(self):
        """Run the complete store-wise analysis."""
        print("COMPREHENSIVE STORE-WISE ANALYSIS")
        print("="*60)
        print(f"Pre-TODC Period: {self.pre_todc_start} to {self.pre_todc_end}")
        print(f"Post-TODC Period: {self.post_todc_start} to {self.post_todc_end}")
        
        # Analyze pre-TODC period
        pre_metrics = self.analyze_store_performance_by_period("Pre-TODC", self.pre_todc_start, self.pre_todc_end)
        
        # Analyze post-TODC period
        post_metrics = self.analyze_store_performance_by_period("Post-TODC", self.post_todc_start, self.post_todc_end)
        
        # Calculate growth metrics
        growth_metrics = self.calculate_store_growth_metrics(pre_metrics, post_metrics)
        
        # Generate insights and recommendations
        insights = self.generate_store_insights(growth_metrics)
        
        # Create visualizations
        self.create_store_visualizations(growth_metrics, insights)
        
        # Export to Excel
        excel_file = self.export_to_excel(pre_metrics, post_metrics, growth_metrics, insights)
        
        print("\n" + "="*60)
        print("STORE-WISE ANALYSIS COMPLETE!")
        print("="*60)
        print(f"Results exported to: {excel_file}")
        
        return {
            'pre_metrics': pre_metrics,
            'post_metrics': post_metrics,
            'growth_metrics': growth_metrics,
            'insights': insights,
            'excel_file': excel_file
        }

def main():
    """Main function to run the store-wise analysis."""
    try:
        # Initialize analyzer
        analyzer = StoreWiseAnalyzer()
        
        # Run complete analysis
        results = analyzer.run_complete_analysis()
        
        print("\nStore-wise analysis completed successfully!")
        print("Check the generated Excel file for detailed store-wise results.")
        
        return results
        
    except Exception as e:
        print(f"Error during store-wise analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
