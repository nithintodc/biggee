#!/usr/bin/env python3
"""
TODC Impact Analysis Script
===========================

This script analyzes the impact of TODC (online restaurant management company) 
on DoorDash client accounts by comparing pre-TODC vs post-TODC performance.

Analysis Periods:
- Pre-TODC: 2025-05-09 to 2025-07-08
- Post-TODC: 2025-07-09 to 2025-09-08
- Year-over-Year: 2024 vs 2025 data

Author: TODC Analytics Team
Date: 2025-09-27

COLUMN NAMES REFERENCE:
======================

Financial Data Columns:
- 'Timestamp UTC date': Date for financial transactions
- 'Subtotal': Order subtotal amount
- 'Commission': Commission charged (negative value)
- 'Marketing fees | (including any applicable taxes)': Marketing fees
- 'Customer discounts from marketing | (funded by you)': Customer discounts funded by merchant
- 'Customer discounts from marketing | (funded by DoorDash)': Customer discounts funded by DoorDash
- 'Net total': Net payout amount
- 'Transaction type': Type of transaction (Order, etc.)

Marketing Data Columns:
- 'Date': Campaign date
- 'Is self serve campaign': Boolean indicating if campaign is self-serve (TRUE/FALSE)
- 'Campaign name': Name of the marketing campaign
- 'Orders': Number of orders from campaign
- 'Sales': Sales generated from campaign
- 'Customer discounts from marketing | (Funded by you)': Customer discounts funded by merchant
- 'Marketing fees | (including any applicable taxes)': Marketing fees
- 'ROAS': Return on Ad Spend

Sales Data Columns:
- 'Start Date': Date for sales data
- 'Gross Sales': Total gross sales
- 'Total Delivered or Picked Up Orders': Number of completed orders
- 'AOV': Average Order Value
- 'Total Commission': Total commission charged
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import os
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Create output directory for charts
if not os.path.exists('charts'):
    os.makedirs('charts')

class TODCAnalyzer:
    def __init__(self):
        """Initialize the TODC Analyzer with data paths and analysis periods."""
        self.data_paths = {
            'financial_2024': 'financial_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z/FINANCIAL_DETAILED_TRANSACTIONS_2024-05-09_2024-09-08_Ir1lF_2025-09-27T17-00-28Z.csv',
            'financial_2025': 'financial_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z/FINANCIAL_DETAILED_TRANSACTIONS_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z.csv',
            'marketing_2024': 'marketing_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z/MARKETING_PROMOTION_2024-05-09_2024-09-08_6bX6R_2025-09-27T17-02-12Z.csv',
            'marketing_2025': 'marketing_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z/MARKETING_PROMOTION_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z.csv',
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
        print("Loading data files...")
        
        # Load financial data
        self.financial_2024 = pd.read_csv(self.data_paths['financial_2024'])
        self.financial_2025 = pd.read_csv(self.data_paths['financial_2025'])
        
        # Load marketing data
        self.marketing_2024 = pd.read_csv(self.data_paths['marketing_2024'])
        self.marketing_2025 = pd.read_csv(self.data_paths['marketing_2025'])
        
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
        print(f"Sales 2024: {len(self.sales_2024):,} records")
        print(f"Sales 2025: {len(self.sales_2025):,} records")
        
    def process_dates(self):
        """Process and standardize date columns across all datasets."""
        # Financial data - use 'Timestamp UTC date' or 'Payout date'
        if 'Timestamp UTC date' in self.financial_2024.columns:
            self.financial_2024['date'] = pd.to_datetime(self.financial_2024['Timestamp UTC date'])
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Timestamp UTC date'])
        elif 'Payout date' in self.financial_2024.columns:
            self.financial_2024['date'] = pd.to_datetime(self.financial_2024['Payout date'])
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Payout date'])
        
        # Marketing data - use 'Date'
        self.marketing_2024['date'] = pd.to_datetime(self.marketing_2024['Date'])
        self.marketing_2025['date'] = pd.to_datetime(self.marketing_2025['Date'])
        
        # Sales data - use 'Start Date'
        self.sales_2024['date'] = pd.to_datetime(self.sales_2024['Start Date'])
        self.sales_2025['date'] = pd.to_datetime(self.sales_2025['Start Date'])
        
    def filter_by_period(self, df, start_date, end_date, date_col='date'):
        """Filter dataframe by date range."""
        return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    
    def calculate_financial_metrics(self):
        """Calculate comprehensive financial metrics for pre/post TODC periods."""
        print("\n" + "="*60)
        print("FINANCIAL ANALYSIS - PRE vs POST TODC")
        print("="*60)
        
        # Filter 2025 data for pre and post TODC periods
        pre_todc_financial = self.filter_by_period(self.financial_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_financial = self.filter_by_period(self.financial_2025, self.post_todc_start, self.post_todc_end)
        
        # Calculate metrics for each period
        pre_metrics = self.calculate_financial_period_metrics(pre_todc_financial, "Pre-TODC")
        post_metrics = self.calculate_financial_period_metrics(post_todc_financial, "Post-TODC")
        
        # Calculate growth metrics
        growth_metrics = self.calculate_growth_metrics(pre_metrics, post_metrics)
        
        return {
            'pre_todc': pre_metrics,
            'post_todc': post_metrics,
            'growth': growth_metrics
        }
    
    def calculate_financial_period_metrics(self, df, period_name):
        """Calculate financial metrics for a specific period."""
        if len(df) == 0:
            return {}
        
        # Filter only successful orders
        orders_df = df[df['Transaction type'] == 'Order']
        
        # Handle different column name formats between 2024 and 2025 data
        marketing_fees_col = None
        customer_discounts_col = None
        dd_discounts_col = None
        
        # Check for 2025 format first
        if 'Marketing fees | (including any applicable taxes)' in df.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in df.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        if 'Customer discounts from marketing | (funded by you)' in df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        elif 'Customer discounts from marketing | (Funded by you)' in df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        
        if 'Customer discounts from marketing | (funded by DoorDash)' in df.columns:
            dd_discounts_col = 'Customer discounts from marketing | (funded by DoorDash)'
        elif 'Customer discounts from marketing | (Funded by DoorDash)' in df.columns:
            dd_discounts_col = 'Customer discounts from marketing | (Funded by DoorDash)'
        
        metrics = {
            'period': period_name,
            'total_orders': len(orders_df),
            'total_subtotal': orders_df['Subtotal'].sum(),
            'total_commission': orders_df['Commission'].abs().sum(),  # Commission is negative
            'total_marketing_fees': orders_df[marketing_fees_col].sum() if marketing_fees_col else 0,
            'total_net_payout': orders_df['Net total'].sum(),
            'avg_order_value': orders_df['Subtotal'].mean(),
            'avg_commission_rate': (orders_df['Commission'].abs().sum() / orders_df['Subtotal'].sum()) * 100,
            'unique_stores': orders_df['Store ID'].nunique(),
            'total_customer_discounts': orders_df[customer_discounts_col].sum() if customer_discounts_col else 0,
            'total_dd_discounts': orders_df[dd_discounts_col].sum() if dd_discounts_col else 0
        }
        
        print(f"\n{period_name} Financial Metrics:")
        print(f"  Total Orders: {metrics['total_orders']:,}")
        print(f"  Total Subtotal: ${metrics['total_subtotal']:,.2f}")
        print(f"  Total Commission: ${metrics['total_commission']:,.2f}")
        print(f"  Total Marketing Fees: ${metrics['total_marketing_fees']:,.2f}")
        print(f"  Total Net Payout: ${metrics['total_net_payout']:,.2f}")
        print(f"  Average Order Value: ${metrics['avg_order_value']:.2f}")
        print(f"  Average Commission Rate: {metrics['avg_commission_rate']:.2f}%")
        print(f"  Unique Stores: {metrics['unique_stores']}")
        
        return metrics
    
    def calculate_growth_metrics(self, pre_metrics, post_metrics):
        """Calculate growth metrics between pre and post TODC periods."""
        growth = {}
        
        for key in pre_metrics:
            if key == 'period':
                continue
            if isinstance(pre_metrics[key], (int, float)) and pre_metrics[key] != 0:
                delta = post_metrics[key] - pre_metrics[key]
                delta_percent = (delta / pre_metrics[key]) * 100
                growth[f'{key}_delta'] = delta
                growth[f'{key}_delta_percent'] = delta_percent
        
        print(f"\nGrowth Analysis (Post-TODC vs Pre-TODC):")
        for key, value in growth.items():
            if 'delta_percent' in key:
                metric_name = key.replace('_delta_percent', '').replace('_', ' ').title()
                delta_value = growth[key.replace('_delta_percent', '_delta')]
                print(f"  {metric_name}:")
                print(f"    Delta: {delta_value:+,.2f}")
                print(f"    Delta %: {value:+.2f}%")
        
        return growth
    
    def analyze_marketing_campaigns(self):
        """Analyze marketing campaign performance and ROI."""
        print("\n" + "="*60)
        print("MARKETING CAMPAIGN ANALYSIS")
        print("="*60)
        
        # Filter marketing data for pre and post TODC periods
        pre_todc_marketing = self.filter_by_period(self.marketing_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_marketing = self.filter_by_period(self.marketing_2025, self.post_todc_start, self.post_todc_end)
        
        # Analyze campaign performance
        pre_campaigns = self.analyze_campaign_performance(pre_todc_marketing, "Pre-TODC")
        post_campaigns = self.analyze_campaign_performance(post_todc_marketing, "Post-TODC")
        
        # Calculate ROI metrics
        roi_analysis = self.calculate_marketing_roi(pre_todc_marketing, post_todc_marketing)
        
        return {
            'pre_campaigns': pre_campaigns,
            'post_campaigns': post_campaigns,
            'roi_analysis': roi_analysis
        }
    
    def analyze_campaign_performance(self, df, period_name):
        """Analyze individual campaign performance."""
        if len(df) == 0:
            return {}
        
        # Filter only self-serve campaigns (SELFSERVE = TRUE)
        if 'Is self serve campaign' in df.columns:
            df = df[df['Is self serve campaign'] == True]
            print(f"  Filtered to {len(df)} self-serve campaign records for {period_name}")
        
        if len(df) == 0:
            print(f"  No self-serve campaigns found for {period_name}")
            return {}
        
        # Handle different column name formats
        customer_discounts_col = None
        marketing_fees_col = None
        
        if 'Customer discounts from marketing | (Funded by you)' in df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        elif 'Customer discounts from marketing | (funded by you)' in df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        
        if 'Marketing fees | (including any applicable taxes)' in df.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in df.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        agg_dict = {
            'Orders': 'sum',
            'Sales': 'sum',
            'ROAS': 'mean',
            'New customers acquired': 'sum',
            'Total customers acquired': 'sum'
        }
        
        if customer_discounts_col:
            agg_dict[customer_discounts_col] = 'sum'
        if marketing_fees_col:
            agg_dict[marketing_fees_col] = 'sum'
        
        campaign_summary = df.groupby('Campaign name').agg(agg_dict).round(2)
        
        # Calculate total cost
        total_cost = 0
        if customer_discounts_col and marketing_fees_col:
            total_cost = campaign_summary[customer_discounts_col] + campaign_summary[marketing_fees_col]
        elif customer_discounts_col:
            total_cost = campaign_summary[customer_discounts_col]
        elif marketing_fees_col:
            total_cost = campaign_summary[marketing_fees_col]
        
        campaign_summary['Total_Cost'] = total_cost
        
        campaign_summary['ROI'] = (
            (campaign_summary['Sales'] - campaign_summary['Total_Cost']) / 
            campaign_summary['Total_Cost'] * 100
        ).round(2)
        
        print(f"\n{period_name} Campaign Performance:")
        print(campaign_summary.sort_values('ROI', ascending=False))
        
        return campaign_summary
    
    def calculate_marketing_roi(self, pre_df, post_df):
        """Calculate overall marketing ROI metrics."""
        # Handle different column name formats
        def get_marketing_cost(df):
            customer_discounts = 0
            marketing_fees = 0
            
            if 'Customer discounts from marketing | (Funded by you)' in df.columns:
                customer_discounts = df['Customer discounts from marketing | (Funded by you)'].sum()
            elif 'Customer discounts from marketing | (funded by you)' in df.columns:
                customer_discounts = df['Customer discounts from marketing | (funded by you)'].sum()
            
            if 'Marketing fees | (including any applicable taxes)' in df.columns:
                marketing_fees = df['Marketing fees | (including any applicable taxes)'].sum()
            elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in df.columns:
                marketing_fees = df['Marketing fees (for historical reference only) | (all discounts and fees)'].sum()
            
            return customer_discounts + marketing_fees
        
        pre_total_cost = get_marketing_cost(pre_df)
        pre_total_sales = pre_df['Sales'].sum()
        pre_roi = ((pre_total_sales - pre_total_cost) / pre_total_cost * 100) if pre_total_cost > 0 else 0
        
        post_total_cost = get_marketing_cost(post_df)
        post_total_sales = post_df['Sales'].sum()
        post_roi = ((post_total_sales - post_total_cost) / post_total_cost * 100) if post_total_cost > 0 else 0
        
        roi_metrics = {
            'pre_todc_roi': pre_roi,
            'post_todc_roi': post_roi,
            'roi_improvement': post_roi - pre_roi,
            'pre_total_cost': pre_total_cost,
            'post_total_cost': post_total_cost,
            'pre_total_sales': pre_total_sales,
            'post_total_sales': post_total_sales
        }
        
        print(f"\nMarketing ROI Analysis:")
        print(f"  Pre-TODC ROI: {pre_roi:.2f}%")
        print(f"  Post-TODC ROI: {post_roi:.2f}%")
        print(f"  ROI Improvement: {post_roi - pre_roi:+.2f}%")
        print(f"  Pre-TODC Total Cost: ${pre_total_cost:,.2f}")
        print(f"  Post-TODC Total Cost: ${post_total_cost:,.2f}")
        print(f"  Pre-TODC Total Sales: ${pre_total_sales:,.2f}")
        print(f"  Post-TODC Total Sales: ${post_total_sales:,.2f}")
        
        return roi_metrics
    
    def analyze_store_performance(self):
        """Analyze store-level performance across periods."""
        print("\n" + "="*60)
        print("STORE PERFORMANCE ANALYSIS")
        print("="*60)
        
        # Filter sales data for pre and post TODC periods
        pre_todc_sales = self.filter_by_period(self.sales_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_sales = self.filter_by_period(self.sales_2025, self.post_todc_start, self.post_todc_end)
        
        # Analyze store performance
        pre_store_performance = self.analyze_store_period_performance(pre_todc_sales, "Pre-TODC")
        post_store_performance = self.analyze_store_period_performance(post_todc_sales, "Post-TODC")
        
        # Calculate store growth
        store_growth = self.calculate_store_growth(pre_store_performance, post_store_performance)
        
        return {
            'pre_performance': pre_store_performance,
            'post_performance': post_store_performance,
            'store_growth': store_growth
        }
    
    def analyze_store_period_performance(self, df, period_name):
        """Analyze store performance for a specific period."""
        if len(df) == 0:
            return {}
        
        store_summary = df.groupby(['Store ID', 'Store Name']).agg({
            'Gross Sales': 'sum',
            'Total Delivered or Picked Up Orders': 'sum',
            'AOV': 'mean',
            'Total Commission': 'sum'
        }).round(2)
        
        store_summary['Net_Revenue'] = store_summary['Gross Sales'] - store_summary['Total Commission']
        
        print(f"\n{period_name} Store Performance (Top 10 by Gross Sales):")
        top_stores = store_summary.sort_values('Gross Sales', ascending=False).head(10)
        print(top_stores)
        
        return store_summary
    
    def calculate_store_growth(self, pre_performance, post_performance):
        """Calculate growth metrics for each store."""
        if len(pre_performance) == 0 or len(post_performance) == 0:
            return {}
        
        # Merge pre and post performance
        merged = pre_performance.merge(
            post_performance, 
            left_index=True, 
            right_index=True, 
            suffixes=('_pre', '_post')
        )
        
        # Calculate growth metrics
        growth_metrics = {}
        for metric in ['Gross Sales', 'Total Delivered or Picked Up Orders', 'AOV', 'Net_Revenue']:
            pre_col = f'{metric}_pre'
            post_col = f'{metric}_post'
            
            if pre_col in merged.columns and post_col in merged.columns:
                merged[f'{metric}_growth'] = (
                    (merged[post_col] - merged[pre_col]) / merged[pre_col] * 100
                ).round(2)
                growth_metrics[metric] = merged[f'{metric}_growth']
        
        print(f"\nStore Growth Analysis (Top 10 by Sales Growth):")
        if 'Gross Sales_growth' in merged.columns:
            top_growth = merged.sort_values('Gross Sales_growth', ascending=False).head(10)
            print(top_growth[['Gross Sales_pre', 'Gross Sales_post', 'Gross Sales_growth']])
        
        return merged
    
    def analyze_weekly_metrics(self):
        """Analyze week-wise metrics for sales, net payout, marketing spend, and customer discounts."""
        print("\n" + "="*60)
        print("WEEK-WISE ANALYSIS")
        print("="*60)
        
        # Define week periods starting from 5/9
        week_periods = {}
        start_date = pd.to_datetime('2025-05-09')
        current_date = start_date
        week_num = 1
        
        while current_date <= pd.to_datetime('2025-09-08'):
            week_end = current_date + pd.Timedelta(days=6)
            if week_end > pd.to_datetime('2025-09-08'):
                week_end = pd.to_datetime('2025-09-08')
            
            week_name = f"Week {week_num}"
            week_periods[week_name] = (current_date.strftime('%Y-%m-%d'), week_end.strftime('%Y-%m-%d'))
            
            current_date = week_end + pd.Timedelta(days=1)
            week_num += 1
        
        weekly_data = []
        
        for week_name, (start_date, end_date) in week_periods.items():
            print(f"\nAnalyzing {week_name} ({start_date} to {end_date})...")
            
            # Filter financial data for the week
            week_financial = self.filter_by_period(self.financial_2025, start_date, end_date)
            week_marketing = self.filter_by_period(self.marketing_2025, start_date, end_date)
            week_sales = self.filter_by_period(self.sales_2025, start_date, end_date)
            
            # Calculate metrics for the week
            week_metrics = self.calculate_weekly_metrics(week_financial, week_marketing, week_sales, week_name, start_date, end_date)
            weekly_data.append(week_metrics)
            
            # Print week summary
            print(f"  Sales: ${week_metrics['sales']:,.2f}")
            print(f"  Net Payout: ${week_metrics['net_payout']:,.2f}")
            print(f"  Marketing Spend: ${week_metrics['marketing_spend']:,.2f}")
            print(f"  Customer Discounts: ${week_metrics['customer_discounts']:,.2f}")
        
        return weekly_data
    
    def calculate_weekly_metrics(self, financial_df, marketing_df, sales_df, week_name, start_date, end_date):
        """Calculate comprehensive metrics for a specific week including all financial components."""
        # Sales (from sales data)
        sales = sales_df['Gross Sales'].sum() if len(sales_df) > 0 else 0
        
        # Net Payout (from financial data - only orders)
        orders_df = financial_df[financial_df['Transaction type'] == 'Order'] if len(financial_df) > 0 else pd.DataFrame()
        net_payout = orders_df['Net total'].sum() if len(orders_df) > 0 else 0
        
        # Marketing Spend (marketing fees + customer discounts funded by you)
        marketing_spend = 0
        if len(marketing_df) > 0:
            marketing_fees = marketing_df['Marketing fees | (including any applicable taxes)'].sum()
            customer_discounts = marketing_df['Customer discounts from marketing | (Funded by you)'].sum()
            marketing_spend = marketing_fees + customer_discounts
        
        # Customer Discounts (funded by you)
        customer_discounts = 0
        if len(marketing_df) > 0:
            customer_discounts = marketing_df['Customer discounts from marketing | (Funded by you)'].sum()
        
        # Comprehensive financial metrics from orders
        comprehensive_metrics = {}
        if len(orders_df) > 0:
            comprehensive_metrics = {
                'subtotal': orders_df['Subtotal'].sum(),
                'commission': orders_df['Commission'].abs().sum(),  # Commission is negative
                'marketing_fees': orders_df['Marketing fees | (including any applicable taxes)'].sum() if 'Marketing fees | (including any applicable taxes)' in orders_df.columns else 0,
                'customer_discounts_funded_by_you': orders_df['Customer discounts from marketing | (funded by you)'].sum() if 'Customer discounts from marketing | (funded by you)' in orders_df.columns else 0,
                'customer_discounts_funded_by_dd': orders_df['Customer discounts from marketing | (funded by DoorDash)'].sum() if 'Customer discounts from marketing | (funded by DoorDash)' in orders_df.columns else 0,
                'net_total': orders_df['Net total'].sum(),
                'total_orders': len(orders_df),
                'avg_order_value': orders_df['Subtotal'].mean()
            }
        
        return {
            'week': week_name,
            'start_date': start_date,
            'end_date': end_date,
            'sales': sales,
            'net_payout': net_payout,
            'marketing_spend': marketing_spend,
            'customer_discounts': customer_discounts,
            **comprehensive_metrics
        }
    
    def analyze_comprehensive_pre_post_metrics(self):
        """Analyze comprehensive pre vs post metrics for all financial components between subtotal and net total."""
        print("\n" + "="*60)
        print("COMPREHENSIVE PRE vs POST ANALYSIS")
        print("="*60)
        
        # Filter data for pre and post TODC periods
        pre_todc_financial = self.filter_by_period(self.financial_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_financial = self.filter_by_period(self.financial_2025, self.post_todc_start, self.post_todc_end)
        
        # Filter only orders for detailed analysis
        pre_orders = pre_todc_financial[pre_todc_financial['Transaction type'] == 'Order']
        post_orders = post_todc_financial[post_todc_financial['Transaction type'] == 'Order']
        
        # Calculate comprehensive metrics for pre-TODC
        pre_metrics = self.calculate_comprehensive_financial_metrics(pre_orders, "Pre-TODC")
        
        # Calculate comprehensive metrics for post-TODC
        post_metrics = self.calculate_comprehensive_financial_metrics(post_orders, "Post-TODC")
        
        # Calculate growth metrics
        growth_metrics = self.calculate_comprehensive_growth_metrics(pre_metrics, post_metrics)
        
        return {
            'pre_todc': pre_metrics,
            'post_todc': post_metrics,
            'growth': growth_metrics
        }
    
    def calculate_comprehensive_financial_metrics(self, orders_df, period_name):
        """Calculate comprehensive financial metrics for all components between subtotal and net total."""
        if len(orders_df) == 0:
            return {}
        
        # Handle different column name formats
        marketing_fees_col = None
        customer_discounts_col = None
        dd_discounts_col = None
        
        # Check for 2025 format first
        if 'Marketing fees | (including any applicable taxes)' in orders_df.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in orders_df.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        if 'Customer discounts from marketing | (funded by you)' in orders_df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        elif 'Customer discounts from marketing | (Funded by you)' in orders_df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        
        if 'Customer discounts from marketing | (funded by DoorDash)' in orders_df.columns:
            dd_discounts_col = 'Customer discounts from marketing | (funded by DoorDash)'
        elif 'Customer discounts from marketing | (Funded by DoorDash)' in orders_df.columns:
            dd_discounts_col = 'Customer discounts from marketing | (Funded by DoorDash)'
        
        metrics = {
            'period': period_name,
            'total_orders': len(orders_df),
            'subtotal': orders_df['Subtotal'].sum(),
            'commission': orders_df['Commission'].abs().sum(),  # Commission is negative
            'marketing_fees': orders_df[marketing_fees_col].sum() if marketing_fees_col else 0,
            'customer_discounts_funded_by_you': orders_df[customer_discounts_col].sum() if customer_discounts_col else 0,
            'customer_discounts_funded_by_dd': orders_df[dd_discounts_col].sum() if dd_discounts_col else 0,
            'net_total': orders_df['Net total'].sum(),
            'avg_order_value': orders_df['Subtotal'].mean(),
            'avg_commission_rate': (orders_df['Commission'].abs().sum() / orders_df['Subtotal'].sum()) * 100,
            'avg_marketing_fee_rate': (orders_df[marketing_fees_col].sum() / orders_df['Subtotal'].sum()) * 100 if marketing_fees_col else 0,
            'avg_customer_discount_rate': (orders_df[customer_discounts_col].sum() / orders_df['Subtotal'].sum()) * 100 if customer_discounts_col else 0,
            'unique_stores': orders_df['Store ID'].nunique()
        }
        
        print(f"\n{period_name} Comprehensive Financial Metrics:")
        print(f"  Total Orders: {metrics['total_orders']:,}")
        print(f"  Subtotal: ${metrics['subtotal']:,.2f}")
        print(f"  Commission: ${metrics['commission']:,.2f} ({metrics['avg_commission_rate']:.2f}%)")
        print(f"  Marketing Fees: ${metrics['marketing_fees']:,.2f} ({metrics['avg_marketing_fee_rate']:.2f}%)")
        print(f"  Customer Discounts (Funded by You): ${metrics['customer_discounts_funded_by_you']:,.2f} ({metrics['avg_customer_discount_rate']:.2f}%)")
        print(f"  Customer Discounts (Funded by DD): ${metrics['customer_discounts_funded_by_dd']:,.2f}")
        print(f"  Net Total: ${metrics['net_total']:,.2f}")
        print(f"  Average Order Value: ${metrics['avg_order_value']:.2f}")
        print(f"  Unique Stores: {metrics['unique_stores']}")
        
        return metrics
    
    def calculate_comprehensive_growth_metrics(self, pre_metrics, post_metrics):
        """Calculate comprehensive growth metrics between pre and post TODC periods."""
        growth = {}
        
        for key in pre_metrics:
            if key == 'period':
                continue
            if isinstance(pre_metrics[key], (int, float)) and pre_metrics[key] != 0:
                delta = post_metrics[key] - pre_metrics[key]
                delta_percent = (delta / pre_metrics[key]) * 100
                growth[f'{key}_delta'] = delta
                growth[f'{key}_delta_percent'] = delta_percent
        
        print(f"\nComprehensive Growth Analysis (Post-TODC vs Pre-TODC):")
        for key, value in growth.items():
            if 'delta_percent' in key:
                metric_name = key.replace('_delta_percent', '').replace('_', ' ').title()
                delta_value = growth[key.replace('_delta_percent', '_delta')]
                print(f"  {metric_name}:")
                print(f"    Delta: {delta_value:+,.2f}")
                print(f"    Delta %: {value:+.2f}%")
        
        return growth
    
    def analyze_self_serve_campaigns_budget_vs_sales(self):
        """Analyze budget vs sales for self-serve campaigns (Is self serve campaign = TRUE)."""
        print("\n" + "="*60)
        print("SELF-SERVE CAMPAIGNS BUDGET VS SALES ANALYSIS")
        print("="*60)
        
        # Filter marketing data for self-serve campaigns only
        self_serve_2024 = self.marketing_2024[self.marketing_2024['Is self serve campaign'] == True].copy()
        self_serve_2025 = self.marketing_2025[self.marketing_2025['Is self serve campaign'] == True].copy()
        
        print(f"Self-serve campaigns 2024: {len(self_serve_2024)} records")
        print(f"Self-serve campaigns 2025: {len(self_serve_2025)} records")
        
        # Handle different column name formats
        def get_campaign_cost(df):
            customer_discounts = 0
            marketing_fees = 0
            
            if 'Customer discounts from marketing | (Funded by you)' in df.columns:
                customer_discounts = df['Customer discounts from marketing | (Funded by you)'].sum()
            elif 'Customer discounts from marketing | (funded by you)' in df.columns:
                customer_discounts = df['Customer discounts from marketing | (funded by you)'].sum()
            
            if 'Marketing fees | (including any applicable taxes)' in df.columns:
                marketing_fees = df['Marketing fees | (including any applicable taxes)'].sum()
            elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in df.columns:
                marketing_fees = df['Marketing fees (for historical reference only) | (all discounts and fees)'].sum()
            
            return customer_discounts + marketing_fees
        
        # Calculate metrics for 2024 self-serve campaigns
        self_serve_2024_metrics = {
            'year': 2024,
            'total_campaigns': len(self_serve_2024),
            'total_orders': self_serve_2024['Orders'].sum() if len(self_serve_2024) > 0 else 0,
            'total_sales': self_serve_2024['Sales'].sum() if len(self_serve_2024) > 0 else 0,
            'total_budget': get_campaign_cost(self_serve_2024),
            'avg_roas': self_serve_2024['ROAS'].mean() if len(self_serve_2024) > 0 else 0,
            'unique_campaigns': self_serve_2024['Campaign name'].nunique() if len(self_serve_2024) > 0 else 0
        }
        
        # Calculate metrics for 2025 self-serve campaigns
        self_serve_2025_metrics = {
            'year': 2025,
            'total_campaigns': len(self_serve_2025),
            'total_orders': self_serve_2025['Orders'].sum() if len(self_serve_2025) > 0 else 0,
            'total_sales': self_serve_2025['Sales'].sum() if len(self_serve_2025) > 0 else 0,
            'total_budget': get_campaign_cost(self_serve_2025),
            'avg_roas': self_serve_2025['ROAS'].mean() if len(self_serve_2025) > 0 else 0,
            'unique_campaigns': self_serve_2025['Campaign name'].nunique() if len(self_serve_2025) > 0 else 0
        }
        
        # Calculate year-over-year growth
        yoy_growth = {}
        for key in self_serve_2024_metrics:
            if key == 'year':
                continue
            if isinstance(self_serve_2024_metrics[key], (int, float)) and self_serve_2024_metrics[key] != 0:
                delta = self_serve_2025_metrics[key] - self_serve_2024_metrics[key]
                delta_percent = (delta / self_serve_2024_metrics[key]) * 100
                yoy_growth[f'{key}_delta'] = delta
                yoy_growth[f'{key}_delta_percent'] = delta_percent
        
        print(f"\nSelf-Serve Campaigns Summary:")
        print(f"2024: {self_serve_2024_metrics['total_campaigns']} campaigns, ${self_serve_2024_metrics['total_budget']:,.2f} budget, ${self_serve_2024_metrics['total_sales']:,.2f} sales")
        print(f"2025: {self_serve_2025_metrics['total_campaigns']} campaigns, ${self_serve_2025_metrics['total_budget']:,.2f} budget, ${self_serve_2025_metrics['total_sales']:,.2f} sales")
        
        # Create detailed campaign analysis for 2025 (more recent data) with proper pivot
        if len(self_serve_2025) > 0:
            # Create pivot table with all requested columns
            pivot_columns = {
                'Orders': 'sum',
                'Sales': 'sum',
                'New customers acquired': 'sum',
                'Total customers acquired': 'sum'
            }
            
            # Add customer discounts column if it exists
            if 'Customer discounts from marketing | (Funded by you)' in self_serve_2025.columns:
                pivot_columns['Customer discounts from marketing | (Funded by you)'] = 'sum'
            elif 'Customer discounts from marketing | (funded by you)' in self_serve_2025.columns:
                pivot_columns['Customer discounts from marketing | (funded by you)'] = 'sum'
            
            # Add marketing fees column if it exists
            if 'Marketing fees | (including any applicable taxes)' in self_serve_2025.columns:
                pivot_columns['Marketing fees | (including any applicable taxes)'] = 'sum'
            elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in self_serve_2025.columns:
                pivot_columns['Marketing fees (for historical reference only) | (all discounts and fees)'] = 'sum'
            
            # Add new DP customers acquired if it exists
            if 'New DP customers acquired' in self_serve_2025.columns:
                pivot_columns['New DP customers acquired'] = 'sum'
            
            # Create the pivot table
            campaign_analysis = self_serve_2025.groupby('Campaign name').agg(pivot_columns).round(2)
            
            # Calculate total budget and ROI
            campaign_budgets = self_serve_2025.groupby('Campaign name').apply(get_campaign_cost)
            campaign_analysis['Total_Budget'] = campaign_budgets
            
            # Calculate ROI
            campaign_analysis['ROI'] = (
                (campaign_analysis['Sales'] - campaign_analysis['Total_Budget']) / 
                campaign_analysis['Total_Budget'] * 100
            ).round(2)
            
            # Sort by ROI descending
            campaign_analysis = campaign_analysis.sort_values('ROI', ascending=False)
            
            print(f"\nTop 10 Self-Serve Campaigns by ROI (2025):")
            print(campaign_analysis.head(10))
            
            return {
                'summary_2024': self_serve_2024_metrics,
                'summary_2025': self_serve_2025_metrics,
                'yoy_growth': yoy_growth,
                'detailed_campaigns_2025': campaign_analysis
            }
        else:
            return {
                'summary_2024': self_serve_2024_metrics,
                'summary_2025': self_serve_2025_metrics,
                'yoy_growth': yoy_growth,
                'detailed_campaigns_2025': pd.DataFrame()
            }
    
    def analyze_store_level_metrics(self):
        """Analyze store-level metrics for pre vs post TODC periods with delta calculations."""
        print("\n" + "="*60)
        print("STORE-LEVEL METRICS ANALYSIS")
        print("="*60)
        
        # Filter data for pre and post TODC periods
        pre_todc_financial = self.filter_by_period(self.financial_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_financial = self.filter_by_period(self.financial_2025, self.post_todc_start, self.post_todc_end)
        
        pre_todc_marketing = self.filter_by_period(self.marketing_2025, self.pre_todc_start, self.pre_todc_end)
        post_todc_marketing = self.filter_by_period(self.marketing_2025, self.post_todc_start, self.post_todc_end)
        
        # Calculate store-level metrics for pre-TODC
        pre_store_metrics = self.calculate_store_level_metrics(pre_todc_financial, pre_todc_marketing, "Pre-TODC")
        
        # Calculate store-level metrics for post-TODC
        post_store_metrics = self.calculate_store_level_metrics(post_todc_financial, post_todc_marketing, "Post-TODC")
        
        # Create comprehensive comparison table with delta calculations
        store_comparison = self.create_store_comparison_table(pre_store_metrics, post_store_metrics)
        
        return {
            'pre_metrics': pre_store_metrics,
            'post_metrics': post_store_metrics,
            'comparison_table': store_comparison
        }
    
    def calculate_store_level_metrics(self, financial_df, marketing_df, period_name):
        """Calculate store-level metrics for a specific period."""
        print(f"\nCalculating {period_name} store-level metrics...")
        
        # Filter only orders from financial data
        orders_df = financial_df[financial_df['Transaction type'] == 'Order'] if len(financial_df) > 0 else pd.DataFrame()
        
        # Handle different column name formats
        marketing_fees_col = None
        customer_discounts_col = None
        
        if 'Marketing fees | (including any applicable taxes)' in orders_df.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in orders_df.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        if 'Customer discounts from marketing | (funded by you)' in orders_df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        elif 'Customer discounts from marketing | (Funded by you)' in orders_df.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        
        # Calculate financial metrics by store
        if len(orders_df) > 0:
            financial_metrics = orders_df.groupby('Store ID').agg({
                'Subtotal': 'sum',  # Overall sales
                'Net total': 'sum'  # Net payout
            }).round(2)
            
            # Add marketing cost calculation
            if marketing_fees_col and customer_discounts_col:
                financial_metrics['Marketing_Cost'] = (
                    orders_df.groupby('Store ID')[marketing_fees_col].sum() + 
                    orders_df.groupby('Store ID')[customer_discounts_col].sum()
                ).round(2)
            else:
                financial_metrics['Marketing_Cost'] = 0
        else:
            financial_metrics = pd.DataFrame()
        
        # Calculate marketing driven sales by store
        if len(marketing_df) > 0:
            marketing_metrics = marketing_df.groupby('Store ID')['Sales'].sum().round(2)
        else:
            marketing_metrics = pd.Series(dtype=float)
        
        # Combine metrics
        store_metrics = pd.DataFrame()
        
        if len(financial_metrics) > 0:
            store_metrics['Overall_Sales'] = financial_metrics['Subtotal']
            store_metrics['Net_Payout'] = financial_metrics['Net total']
            store_metrics['Marketing_Cost'] = financial_metrics['Marketing_Cost']
        
        if len(marketing_metrics) > 0:
            store_metrics['Marketing_Driven_Sales'] = marketing_metrics
        else:
            store_metrics['Marketing_Driven_Sales'] = 0
        
        # Calculate organic sales (Overall - Marketing Driven)
        if len(store_metrics) > 0:
            store_metrics['Organic_Sales'] = store_metrics['Overall_Sales'] - store_metrics['Marketing_Driven_Sales']
        
        # Fill NaN values with 0
        store_metrics = store_metrics.fillna(0)
        
        print(f"  {period_name}: {len(store_metrics)} stores analyzed")
        print(f"  Total Overall Sales: ${store_metrics['Overall_Sales'].sum():,.2f}")
        print(f"  Total Marketing Driven Sales: ${store_metrics['Marketing_Driven_Sales'].sum():,.2f}")
        print(f"  Total Organic Sales: ${store_metrics['Organic_Sales'].sum():,.2f}")
        print(f"  Total Marketing Cost: ${store_metrics['Marketing_Cost'].sum():,.2f}")
        print(f"  Total Net Payout: ${store_metrics['Net_Payout'].sum():,.2f}")
        
        return store_metrics
    
    def create_store_comparison_table(self, pre_metrics, post_metrics):
        """Create comprehensive store comparison table with delta calculations."""
        print("\nCreating store comparison table with delta calculations...")
        
        # Get all unique store IDs
        all_stores = set(pre_metrics.index) | set(post_metrics.index)
        
        # Create multi-level column structure
        metrics = ['Overall_Sales', 'Marketing_Driven_Sales', 'Organic_Sales', 'Marketing_Cost', 'Net_Payout']
        periods = ['Pre', 'Post', 'Delta', 'Delta_Percent']
        
        # Create multi-level columns
        columns = pd.MultiIndex.from_product([periods, metrics], names=['Period', 'Metric'])
        
        # Initialize comparison dataframe
        comparison_df = pd.DataFrame(index=sorted(all_stores), columns=columns)
        
        # Fill in pre and post data
        for store_id in all_stores:
            for metric in metrics:
                # Pre-TODC values
                pre_val = pre_metrics.loc[store_id, metric] if store_id in pre_metrics.index else 0
                comparison_df.loc[store_id, ('Pre', metric)] = pre_val
                
                # Post-TODC values
                post_val = post_metrics.loc[store_id, metric] if store_id in post_metrics.index else 0
                comparison_df.loc[store_id, ('Post', metric)] = post_val
                
                # Delta calculation
                delta = post_val - pre_val
                comparison_df.loc[store_id, ('Delta', metric)] = delta
                
                # Delta percentage calculation
                delta_percent = (delta / pre_val * 100) if pre_val != 0 else 0
                comparison_df.loc[store_id, ('Delta_Percent', metric)] = delta_percent
        
        # Round to 2 decimal places
        comparison_df = comparison_df.round(2)
        
        print(f"Store comparison table created with {len(comparison_df)} stores and {len(metrics)} metrics")
        
        # Print summary statistics
        print(f"\nStore-Level Summary:")
        for metric in metrics:
            pre_total = comparison_df[('Pre', metric)].sum()
            post_total = comparison_df[('Post', metric)].sum()
            delta_total = comparison_df[('Delta', metric)].sum()
            delta_percent_total = (delta_total / pre_total * 100) if pre_total != 0 else 0
            
            print(f"  {metric}:")
            print(f"    Pre-TODC: ${pre_total:,.2f}")
            print(f"    Post-TODC: ${post_total:,.2f}")
            print(f"    Delta: ${delta_total:+,.2f} ({delta_percent_total:+.2f}%)")
        
        return comparison_df
    
    def create_visualizations(self, financial_analysis, marketing_analysis, store_analysis, yoy_analysis):
        """Create various visualizations for the analysis."""
        print("\n" + "="*60)
        print("CREATING VISUALIZATIONS")
        print("="*60)
        
        # Create line graphs for key metrics over time
        self.create_line_graphs()
        
        # Create campaign budget vs sales scatter plot
        self.create_campaign_scatter_plot(marketing_analysis)
        
        # Create store performance comparison charts
        self.create_store_performance_charts(store_analysis)
        
        # Create financial metrics comparison charts
        self.create_financial_comparison_charts(financial_analysis)
        
        # Create week-wise metrics chart
        weekly_data = self.analyze_weekly_metrics()
        self.create_weekly_metrics_chart(weekly_data)
        
        print("All visualizations saved to 'charts' directory")
    
    def create_line_graphs(self):
        """Create line graphs showing trends over time."""
        # Daily sales trend
        daily_sales_2025 = self.sales_2025.groupby('date')['Gross Sales'].sum().reset_index()
        daily_sales_2025['Period'] = daily_sales_2025['date'].apply(
            lambda x: 'Pre-TODC' if x < pd.to_datetime(self.post_todc_start) else 'Post-TODC'
        )
        
        plt.figure(figsize=(15, 8))
        plt.subplot(2, 2, 1)
        for period in ['Pre-TODC', 'Post-TODC']:
            period_data = daily_sales_2025[daily_sales_2025['Period'] == period]
            plt.plot(period_data['date'], period_data['Gross Sales'], 
                    label=period, linewidth=2, marker='o', markersize=4)
        
        plt.title('Daily Sales Trend - 2025', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Gross Sales ($)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Daily orders trend
        daily_orders_2025 = self.sales_2025.groupby('date')['Total Delivered or Picked Up Orders'].sum().reset_index()
        daily_orders_2025['Period'] = daily_orders_2025['date'].apply(
            lambda x: 'Pre-TODC' if x < pd.to_datetime(self.post_todc_start) else 'Post-TODC'
        )
        
        plt.subplot(2, 2, 2)
        for period in ['Pre-TODC', 'Post-TODC']:
            period_data = daily_orders_2025[daily_orders_2025['Period'] == period]
            plt.plot(period_data['date'], period_data['Total Delivered or Picked Up Orders'], 
                    label=period, linewidth=2, marker='s', markersize=4)
        
        plt.title('Daily Orders Trend - 2025', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Total Orders')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # AOV trend
        daily_aov_2025 = self.sales_2025.groupby('date')['AOV'].mean().reset_index()
        daily_aov_2025['Period'] = daily_aov_2025['date'].apply(
            lambda x: 'Pre-TODC' if x < pd.to_datetime(self.post_todc_start) else 'Post-TODC'
        )
        
        plt.subplot(2, 2, 3)
        for period in ['Pre-TODC', 'Post-TODC']:
            period_data = daily_aov_2025[daily_aov_2025['Period'] == period]
            plt.plot(period_data['date'], period_data['AOV'], 
                    label=period, linewidth=2, marker='^', markersize=4)
        
        plt.title('Average Order Value Trend - 2025', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('AOV ($)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Marketing spend trend
        daily_marketing = self.marketing_2025.groupby('date').agg({
            'Customer discounts from marketing | (Funded by you)': 'sum',
            'Marketing fees | (including any applicable taxes)': 'sum'
        }).reset_index()
        daily_marketing['Total_Marketing_Spend'] = (
            daily_marketing['Customer discounts from marketing | (Funded by you)'] + 
            daily_marketing['Marketing fees | (including any applicable taxes)']
        )
        daily_marketing['Period'] = daily_marketing['date'].apply(
            lambda x: 'Pre-TODC' if x < pd.to_datetime(self.post_todc_start) else 'Post-TODC'
        )
        
        plt.subplot(2, 2, 4)
        for period in ['Pre-TODC', 'Post-TODC']:
            period_data = daily_marketing[daily_marketing['Period'] == period]
            plt.plot(period_data['date'], period_data['Total_Marketing_Spend'], 
                    label=period, linewidth=2, marker='d', markersize=4)
        
        plt.title('Daily Marketing Spend Trend - 2025', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Marketing Spend ($)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('charts/line_graphs_trends.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Line graphs saved: charts/line_graphs_trends.png")
    
    def create_campaign_scatter_plot(self, marketing_analysis):
        """Create scatter plot showing campaign budget vs sales."""
        # Combine pre and post campaign data
        all_campaigns = []
        
        if 'pre_campaigns' in marketing_analysis:
            pre_campaigns = marketing_analysis['pre_campaigns'].copy()
            pre_campaigns['Period'] = 'Pre-TODC'
            all_campaigns.append(pre_campaigns)
        
        if 'post_campaigns' in marketing_analysis:
            post_campaigns = marketing_analysis['post_campaigns'].copy()
            post_campaigns['Period'] = 'Post-TODC'
            all_campaigns.append(post_campaigns)
        
        if not all_campaigns:
            return
        
        combined_campaigns = pd.concat(all_campaigns, ignore_index=False)
        
        # Create the scatter plot
        plt.figure(figsize=(14, 10))
        
        # Plot pre and post campaigns with different colors
        for period in ['Pre-TODC', 'Post-TODC']:
            period_data = combined_campaigns[combined_campaigns['Period'] == period]
            if len(period_data) > 0:
                plt.scatter(period_data['Total_Cost'], period_data['Sales'], 
                           label=period, alpha=0.7, s=100, edgecolors='black', linewidth=0.5)
        
        # Add trend line
        if len(combined_campaigns) > 1:
            z = np.polyfit(combined_campaigns['Total_Cost'], combined_campaigns['Sales'], 1)
            p = np.poly1d(z)
            plt.plot(combined_campaigns['Total_Cost'], p(combined_campaigns['Total_Cost']), 
                    "r--", alpha=0.8, linewidth=2, label='Trend Line')
        
        # Add ROI contour lines
        cost_range = np.linspace(combined_campaigns['Total_Cost'].min(), 
                                combined_campaigns['Total_Cost'].max(), 100)
        for roi in [100, 200, 300, 400, 500]:
            sales_roi = cost_range * (1 + roi/100)
            plt.plot(cost_range, sales_roi, '--', alpha=0.3, color='gray', linewidth=1)
            plt.text(cost_range[-1], sales_roi[-1], f'{roi}% ROI', 
                    fontsize=8, alpha=0.7, ha='left')
        
        plt.xlabel('Campaign Budget ($)', fontsize=12, fontweight='bold')
        plt.ylabel('Sales Generated ($)', fontsize=12, fontweight='bold')
        plt.title('Campaign Budget vs Sales Generated\n(Scatter Plot with ROI Contours)', 
                 fontsize=14, fontweight='bold')
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        
        # Add annotations for top performing campaigns
        top_campaigns = combined_campaigns.nlargest(5, 'ROI')
        for idx, (campaign, data) in enumerate(top_campaigns.iterrows()):
            plt.annotate(f'{campaign[:20]}...\nROI: {data["ROI"]:.0f}%', 
                        (data['Total_Cost'], data['Sales']),
                        xytext=(10, 10), textcoords='offset points',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                        fontsize=8, ha='left')
        
        plt.tight_layout()
        plt.savefig('charts/campaign_budget_vs_sales.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Campaign scatter plot saved: charts/campaign_budget_vs_sales.png")
    
    def create_store_performance_charts(self, store_analysis):
        """Create store performance comparison charts."""
        if 'pre_performance' not in store_analysis or 'post_performance' not in store_analysis:
            return
        
        pre_stores = store_analysis['pre_performance']
        post_stores = store_analysis['post_performance']
        
        # Store sales comparison
        plt.figure(figsize=(16, 10))
        
        # Get store names for x-axis
        store_names = [name.split('(')[1].split(')')[0] if '(' in name else name 
                      for name in pre_stores.index.get_level_values('Store Name')]
        
        plt.subplot(2, 2, 1)
        x_pos = np.arange(len(store_names))
        width = 0.35
        
        plt.bar(x_pos - width/2, pre_stores['Gross Sales'], width, 
               label='Pre-TODC', alpha=0.8, color='skyblue')
        plt.bar(x_pos + width/2, post_stores['Gross Sales'], width, 
               label='Post-TODC', alpha=0.8, color='lightcoral')
        
        plt.xlabel('Store')
        plt.ylabel('Gross Sales ($)')
        plt.title('Store Sales Comparison', fontweight='bold')
        plt.xticks(x_pos, store_names, rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Store orders comparison
        plt.subplot(2, 2, 2)
        plt.bar(x_pos - width/2, pre_stores['Total Delivered or Picked Up Orders'], width, 
               label='Pre-TODC', alpha=0.8, color='lightgreen')
        plt.bar(x_pos + width/2, post_stores['Total Delivered or Picked Up Orders'], width, 
               label='Post-TODC', alpha=0.8, color='orange')
        
        plt.xlabel('Store')
        plt.ylabel('Total Orders')
        plt.title('Store Orders Comparison', fontweight='bold')
        plt.xticks(x_pos, store_names, rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Store AOV comparison
        plt.subplot(2, 2, 3)
        plt.bar(x_pos - width/2, pre_stores['AOV'], width, 
               label='Pre-TODC', alpha=0.8, color='plum')
        plt.bar(x_pos + width/2, post_stores['AOV'], width, 
               label='Post-TODC', alpha=0.8, color='gold')
        
        plt.xlabel('Store')
        plt.ylabel('Average Order Value ($)')
        plt.title('Store AOV Comparison', fontweight='bold')
        plt.xticks(x_pos, store_names, rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Growth percentage
        plt.subplot(2, 2, 4)
        growth_percent = ((post_stores['Gross Sales'] - pre_stores['Gross Sales']) / 
                         pre_stores['Gross Sales'] * 100)
        colors = ['green' if x > 0 else 'red' for x in growth_percent]
        
        plt.bar(x_pos, growth_percent, color=colors, alpha=0.7)
        plt.xlabel('Store')
        plt.ylabel('Sales Growth (%)')
        plt.title('Store Sales Growth %', fontweight='bold')
        plt.xticks(x_pos, store_names, rotation=45, ha='right')
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('charts/store_performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Store performance charts saved: charts/store_performance_comparison.png")
    
    def create_financial_comparison_charts(self, financial_analysis):
        """Create financial metrics comparison charts."""
        if 'pre_todc' not in financial_analysis or 'post_todc' not in financial_analysis:
            return
        
        pre_metrics = financial_analysis['pre_todc']
        post_metrics = financial_analysis['post_todc']
        
        # Key metrics comparison
        metrics_to_plot = [
            ('total_orders', 'Total Orders'),
            ('total_subtotal', 'Total Revenue ($)'),
            ('total_net_payout', 'Net Payout ($)'),
            ('avg_order_value', 'Average Order Value ($)')
        ]
        
        plt.figure(figsize=(15, 10))
        
        for i, (metric_key, metric_name) in enumerate(metrics_to_plot, 1):
            plt.subplot(2, 2, i)
            
            pre_val = pre_metrics.get(metric_key, 0)
            post_val = post_metrics.get(metric_key, 0)
            
            bars = plt.bar(['Pre-TODC', 'Post-TODC'], [pre_val, post_val], 
                          color=['skyblue', 'lightcoral'], alpha=0.8, edgecolor='black')
            
            # Add value labels on bars
            for bar, val in zip(bars, [pre_val, post_val]):
                height = bar.get_height()
                if 'total' in metric_key:
                    plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                            f'{val:,.0f}', ha='center', va='bottom', fontweight='bold')
                else:
                    plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                            f'{val:.2f}', ha='center', va='bottom', fontweight='bold')
            
            # Calculate and display growth percentage
            if pre_val > 0:
                growth = ((post_val - pre_val) / pre_val) * 100
                plt.title(f'{metric_name}\nGrowth: {growth:+.1f}%', fontweight='bold')
            else:
                plt.title(metric_name, fontweight='bold')
            
            plt.ylabel(metric_name)
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('charts/financial_metrics_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Financial comparison charts saved: charts/financial_metrics_comparison.png")
    
    def create_weekly_metrics_chart(self, weekly_data):
        """Create week-wise metrics chart showing sales, net payout, marketing spend, and customer discounts."""
        if not weekly_data:
            print("No weekly data available for visualization")
            return
        
        # Convert to DataFrame for easier plotting
        df = pd.DataFrame(weekly_data)
        
        # Create the chart
        plt.figure(figsize=(20, 12))
        
        # Sales chart
        plt.subplot(2, 2, 1)
        bars1 = plt.bar(df['week'], df['sales'], color='skyblue', alpha=0.8, edgecolor='black')
        plt.title('Weekly Sales', fontsize=14, fontweight='bold')
        plt.ylabel('Sales ($)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars1, df['sales']):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'${val:,.0f}', ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        # Net Payout chart
        plt.subplot(2, 2, 2)
        bars2 = plt.bar(df['week'], df['net_payout'], color='lightgreen', alpha=0.8, edgecolor='black')
        plt.title('Weekly Net Payout', fontsize=14, fontweight='bold')
        plt.ylabel('Net Payout ($)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars2, df['net_payout']):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'${val:,.0f}', ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        # Marketing Spend chart
        plt.subplot(2, 2, 3)
        bars3 = plt.bar(df['week'], df['marketing_spend'], color='orange', alpha=0.8, edgecolor='black')
        plt.title('Weekly Marketing Spend', fontsize=14, fontweight='bold')
        plt.ylabel('Marketing Spend ($)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars3, df['marketing_spend']):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'${val:,.0f}', ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        # Customer Discounts chart
        plt.subplot(2, 2, 4)
        bars4 = plt.bar(df['week'], df['customer_discounts'], color='lightcoral', alpha=0.8, edgecolor='black')
        plt.title('Weekly Customer Discounts (Funded by You)', fontsize=14, fontweight='bold')
        plt.ylabel('Customer Discounts ($)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars4, df['customer_discounts']):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'${val:,.0f}', ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('charts/weekly_metrics_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Weekly metrics chart saved: charts/weekly_metrics_comparison.png")
        
        # Create a combined line chart showing only sales and net payout (weeks 1-17)
        plt.figure(figsize=(20, 10))
        
        # Filter to weeks 1-17 only
        df_filtered = df[df['week'].isin([f'Week {i}' for i in range(1, 18)])].copy()
        
        # Plot sales and net payout without normalization
        plt.plot(df_filtered['week'], df_filtered['sales'], marker='o', linewidth=3, markersize=8, 
                label='Sales', color='blue')
        plt.plot(df_filtered['week'], df_filtered['net_payout'], marker='s', linewidth=3, markersize=8, 
                label='Net Payout', color='green')
        
        plt.title('Weekly Sales and Net Payout Trends (Weeks 1-17)', fontsize=16, fontweight='bold')
        plt.xlabel('Week', fontsize=12, fontweight='bold')
        plt.ylabel('Amount ($)', fontsize=12, fontweight='bold')
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        # Add value labels on the lines for key weeks
        for i, (week, row) in enumerate(df_filtered.iterrows()):
            if i % 3 == 0:  # Show every third week to avoid clutter
                plt.annotate(f'Sales: ${row["sales"]:,.0f}\nNet Payout: ${row["net_payout"]:,.0f}',
                            xy=(i, row["sales"]), xytext=(10, 10), textcoords='offset points',
                            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.7),
                            fontsize=9, ha='left')
        
        # Add a vertical line to show TODC implementation (Week 9)
        plt.axvline(x=8, color='red', linestyle='--', alpha=0.7, linewidth=2)
        plt.text(8.2, max(df_filtered['sales'].max(), df_filtered['net_payout'].max()) * 0.9, 
                'TODC Implementation\n(Week 9)', fontsize=10, color='red', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('charts/weekly_metrics_trends.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("Weekly metrics trends chart saved: charts/weekly_metrics_trends.png")
    
    def year_over_year_analysis(self):
        """Perform year-over-year analysis comparing 2024 vs 2025 for both pre and post TODC periods."""
        print("\n" + "="*60)
        print("YEAR-OVER-YEAR ANALYSIS (2024 vs 2025)")
        print("="*60)
        
        # Pre-TODC Period YoY Analysis
        print("\n" + "-"*40)
        print("PRE-TODC PERIOD YoY ANALYSIS")
        print("-"*40)
        print(f"Comparing: 2024-05-09 to 2024-07-08 vs 2025-05-09 to 2025-07-08")
        
        # Filter data for pre-TODC periods
        financial_2024_pre = self.filter_by_period(self.financial_2024, '2024-05-09', '2024-07-08')
        financial_2025_pre = self.filter_by_period(self.financial_2025, '2025-05-09', '2025-07-08')
        
        marketing_2024_pre = self.filter_by_period(self.marketing_2024, '2024-05-09', '2024-07-08')
        marketing_2025_pre = self.filter_by_period(self.marketing_2025, '2025-05-09', '2025-07-08')
        
        sales_2024_pre = self.filter_by_period(self.sales_2024, '2024-05-09', '2024-07-08')
        sales_2025_pre = self.filter_by_period(self.sales_2025, '2025-05-09', '2025-07-08')
        
        # Calculate YoY metrics for pre-TODC period
        yoy_financial_pre = self.calculate_yoy_metrics(financial_2024_pre, financial_2025_pre, "Financial (Pre-TODC)")
        yoy_marketing_pre = self.calculate_yoy_metrics(marketing_2024_pre, marketing_2025_pre, "Marketing (Pre-TODC)")
        yoy_sales_pre = self.calculate_yoy_metrics(sales_2024_pre, sales_2025_pre, "Sales (Pre-TODC)")
        
        # Post-TODC Period YoY Analysis
        print("\n" + "-"*40)
        print("POST-TODC PERIOD YoY ANALYSIS")
        print("-"*40)
        print(f"Comparing: 2024-07-09 to 2024-09-08 vs 2025-07-09 to 2025-09-08")
        
        # Filter data for post-TODC periods
        financial_2024_post = self.filter_by_period(self.financial_2024, '2024-07-09', '2024-09-08')
        financial_2025_post = self.filter_by_period(self.financial_2025, '2025-07-09', '2025-09-08')
        
        marketing_2024_post = self.filter_by_period(self.marketing_2024, '2024-07-09', '2024-09-08')
        marketing_2025_post = self.filter_by_period(self.marketing_2025, '2025-07-09', '2025-09-08')
        
        sales_2024_post = self.filter_by_period(self.sales_2024, '2024-07-09', '2024-09-08')
        sales_2025_post = self.filter_by_period(self.sales_2025, '2025-07-09', '2025-09-08')
        
        # Calculate YoY metrics for post-TODC period
        yoy_financial_post = self.calculate_yoy_metrics(financial_2024_post, financial_2025_post, "Financial (Post-TODC)")
        yoy_marketing_post = self.calculate_yoy_metrics(marketing_2024_post, marketing_2025_post, "Marketing (Post-TODC)")
        yoy_sales_post = self.calculate_yoy_metrics(sales_2024_post, sales_2025_post, "Sales (Post-TODC)")
        
        # Overall Period YoY Analysis
        print("\n" + "-"*40)
        print("OVERALL PERIOD YoY ANALYSIS")
        print("-"*40)
        print(f"Comparing: 2024-05-09 to 2024-09-08 vs 2025-05-09 to 2025-09-08")
        
        # Filter data for overall periods
        financial_2024_overall = self.filter_by_period(self.financial_2024, '2024-05-09', '2024-09-08')
        financial_2025_overall = self.filter_by_period(self.financial_2025, '2025-05-09', '2025-09-08')
        
        marketing_2024_overall = self.filter_by_period(self.marketing_2024, '2024-05-09', '2024-09-08')
        marketing_2025_overall = self.filter_by_period(self.marketing_2025, '2025-05-09', '2025-09-08')
        
        sales_2024_overall = self.filter_by_period(self.sales_2024, '2024-05-09', '2024-09-08')
        sales_2025_overall = self.filter_by_period(self.sales_2025, '2025-05-09', '2025-09-08')
        
        # Calculate YoY metrics for overall period
        yoy_financial_overall = self.calculate_yoy_metrics(financial_2024_overall, financial_2025_overall, "Financial (Overall)")
        yoy_marketing_overall = self.calculate_yoy_metrics(marketing_2024_overall, marketing_2025_overall, "Marketing (Overall)")
        yoy_sales_overall = self.calculate_yoy_metrics(sales_2024_overall, sales_2025_overall, "Sales (Overall)")
        
        return {
            'pre_todc': {
                'financial_yoy': yoy_financial_pre,
                'marketing_yoy': yoy_marketing_pre,
                'sales_yoy': yoy_sales_pre
            },
            'post_todc': {
                'financial_yoy': yoy_financial_post,
                'marketing_yoy': yoy_marketing_post,
                'sales_yoy': yoy_sales_post
            },
            'overall': {
                'financial_yoy': yoy_financial_overall,
                'marketing_yoy': yoy_marketing_overall,
                'sales_yoy': yoy_sales_overall
            }
        }
    
    def calculate_yoy_metrics(self, df_2024, df_2025, data_type):
        """Calculate year-over-year metrics."""
        # Initialize variables
        orders_2024 = 0
        orders_2025 = 0
        sales_2024 = 0
        sales_2025 = 0
        
        if "Financial" in data_type:
            if len(df_2024) > 0:
                orders_2024 = len(df_2024[df_2024['Transaction type'] == 'Order'])
                sales_2024 = df_2024[df_2024['Transaction type'] == 'Order']['Subtotal'].sum()
            if len(df_2025) > 0:
                orders_2025 = len(df_2025[df_2025['Transaction type'] == 'Order'])
                sales_2025 = df_2025[df_2025['Transaction type'] == 'Order']['Subtotal'].sum()
            
        elif "Marketing" in data_type:
            if len(df_2024) > 0:
                orders_2024 = df_2024['Orders'].sum()
                sales_2024 = df_2024['Sales'].sum()
            if len(df_2025) > 0:
                orders_2025 = df_2025['Orders'].sum()
                sales_2025 = df_2025['Sales'].sum()
            
        elif "Sales" in data_type:
            if len(df_2024) > 0:
                orders_2024 = df_2024['Total Delivered or Picked Up Orders'].sum()
                sales_2024 = df_2024['Gross Sales'].sum()
            if len(df_2025) > 0:
                orders_2025 = df_2025['Total Delivered or Picked Up Orders'].sum()
                sales_2025 = df_2025['Gross Sales'].sum()
        
        yoy_metrics = {
            'orders_2024': orders_2024,
            'orders_2025': orders_2025,
            'orders_growth': ((orders_2025 - orders_2024) / orders_2024 * 100) if orders_2024 > 0 else 0,
            'sales_2024': sales_2024,
            'sales_2025': sales_2025,
            'sales_growth': ((sales_2025 - sales_2024) / sales_2024 * 100) if sales_2024 > 0 else 0
        }
        
        print(f"\n{data_type} Year-over-Year Growth:")
        orders_delta = orders_2025 - orders_2024
        sales_delta = sales_2025 - sales_2024
        print(f"  Orders: {orders_2024:,} -> {orders_2025:,}")
        print(f"    Delta: {orders_delta:+,}")
        print(f"    Growth: {yoy_metrics['orders_growth']:+.2f}%")
        print(f"  Sales: ${sales_2024:,.2f} -> ${sales_2025:,.2f}")
        print(f"    Delta: ${sales_delta:+,.2f}")
        print(f"    Growth: {yoy_metrics['sales_growth']:+.2f}%")
        
        return yoy_metrics
    
    def generate_insights_and_recommendations(self, financial_analysis, marketing_analysis, store_analysis, yoy_analysis):
        """Generate key insights and recommendations based on the analysis."""
        print("\n" + "="*60)
        print("KEY INSIGHTS & RECOMMENDATIONS")
        print("="*60)
        
        insights = []
        recommendations = []
        
        # Financial insights
        if 'growth' in financial_analysis:
            growth = financial_analysis['growth']
            if 'total_orders_growth' in growth:
                if growth['total_orders_growth'] > 0:
                    insights.append(f"[POSITIVE] Order volume increased by {growth['total_orders_growth']:.1f}% post-TODC")
                    recommendations.append("Continue current operational strategies that drove order growth")
                else:
                    insights.append(f"[WARNING] Order volume decreased by {abs(growth['total_orders_growth']):.1f}% post-TODC")
                    recommendations.append("Investigate factors causing order decline and implement corrective measures")
            
            if 'avg_order_value_growth' in growth:
                if growth['avg_order_value_growth'] > 0:
                    insights.append(f"[POSITIVE] Average order value increased by {growth['avg_order_value_growth']:.1f}% post-TODC")
                    recommendations.append("Leverage successful upselling strategies across all stores")
                else:
                    insights.append(f"[WARNING] Average order value decreased by {abs(growth['avg_order_value_growth']):.1f}% post-TODC")
                    recommendations.append("Review menu pricing and upselling tactics")
        
        # Marketing insights
        if 'roi_analysis' in marketing_analysis:
            roi = marketing_analysis['roi_analysis']
            if roi['roi_improvement'] > 0:
                insights.append(f"[POSITIVE] Marketing ROI improved by {roi['roi_improvement']:.1f}% post-TODC")
                recommendations.append("Scale successful marketing campaigns and optimize budget allocation")
            else:
                insights.append(f"[WARNING] Marketing ROI decreased by {abs(roi['roi_improvement']):.1f}% post-TODC")
                recommendations.append("Review and optimize underperforming marketing campaigns")
        
        # Year-over-year insights
        if 'overall' in yoy_analysis and 'financial_yoy' in yoy_analysis['overall']:
            yoy_overall = yoy_analysis['overall']['financial_yoy']
            if yoy_overall['sales_growth'] > 0:
                insights.append(f"[POSITIVE] Overall year-over-year sales growth of {yoy_overall['sales_growth']:.1f}%")
                recommendations.append("Maintain momentum with proven strategies")
            else:
                insights.append(f"[WARNING] Overall year-over-year sales decline of {abs(yoy_overall['sales_growth']):.1f}%")
                recommendations.append("Implement aggressive growth strategies to reverse decline")
        
        # Pre vs Post TODC YoY comparison
        if 'pre_todc' in yoy_analysis and 'post_todc' in yoy_analysis:
            pre_yoy = yoy_analysis['pre_todc']['financial_yoy']
            post_yoy = yoy_analysis['post_todc']['financial_yoy']
            
            if pre_yoy['sales_growth'] > 0 and post_yoy['sales_growth'] > 0:
                if post_yoy['sales_growth'] > pre_yoy['sales_growth']:
                    insights.append(f"[POSITIVE] Post-TODC YoY growth ({post_yoy['sales_growth']:.1f}%) exceeds Pre-TODC YoY growth ({pre_yoy['sales_growth']:.1f}%)")
                    recommendations.append("TODC implementation accelerated year-over-year growth performance")
                else:
                    insights.append(f"[WARNING] Post-TODC YoY growth ({post_yoy['sales_growth']:.1f}%) lower than Pre-TODC YoY growth ({pre_yoy['sales_growth']:.1f}%)")
                    recommendations.append("Investigate factors affecting post-TODC year-over-year performance")
        
        print("\nKey Insights:")
        for insight in insights:
            print(f"  {insight}")
        
        print("\nRecommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        
        return {
            'insights': insights,
            'recommendations': recommendations
        }
    
    def export_to_excel(self, financial_analysis, marketing_analysis, store_analysis, yoy_analysis, insights, comprehensive_analysis=None, weekly_data=None, self_serve_analysis=None, store_level_analysis=None):
        """Export all analysis results to an Excel file with multiple sheets."""
        print("\n" + "="*60)
        print("EXPORTING RESULTS TO EXCEL")
        print("="*60)
        
        filename = f"TODC_Analysis_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Financial Analysis Sheet with Pre/Post Comparison
            if 'pre_todc' in financial_analysis and 'post_todc' in financial_analysis:
                pre_metrics = financial_analysis['pre_todc']
                post_metrics = financial_analysis['post_todc']
                growth_metrics = financial_analysis.get('growth', {})
                
                # Create comparison DataFrame
                financial_comparison = []
                for key in pre_metrics:
                    if key == 'period':
                        continue
                    if isinstance(pre_metrics[key], (int, float)):
                        pre_val = pre_metrics[key]
                        post_val = post_metrics[key]
                        delta = post_val - pre_val
                        delta_percent = (delta / pre_val * 100) if pre_val != 0 else 0
                        
                        financial_comparison.append({
                            'Metric': key.replace('_', ' ').title(),
                            'Pre_TODC': pre_val,
                            'Post_TODC': post_val,
                            'Delta': delta,
                            'Delta_Percent': delta_percent
                        })
                
                financial_df = pd.DataFrame(financial_comparison)
                financial_df.to_excel(writer, sheet_name='Financial_Analysis', index=False)
            
            # Marketing Campaign Comparison Sheet
            if 'pre_campaigns' in marketing_analysis and 'post_campaigns' in marketing_analysis:
                pre_campaigns = marketing_analysis['pre_campaigns']
                post_campaigns = marketing_analysis['post_campaigns']
                
                # Get common campaigns
                common_campaigns = set(pre_campaigns.index) & set(post_campaigns.index)
                
                marketing_comparison = []
                for campaign in common_campaigns:
                    pre_orders = pre_campaigns.loc[campaign, 'Orders']
                    post_orders = post_campaigns.loc[campaign, 'Orders']
                    pre_sales = pre_campaigns.loc[campaign, 'Sales']
                    post_sales = post_campaigns.loc[campaign, 'Sales']
                    pre_roi = pre_campaigns.loc[campaign, 'ROI']
                    post_roi = post_campaigns.loc[campaign, 'ROI']
                    
                    marketing_comparison.append({
                        'Campaign_Name': campaign,
                        'Pre_Orders': pre_orders,
                        'Post_Orders': post_orders,
                        'Orders_Delta': post_orders - pre_orders,
                        'Orders_Delta_Percent': ((post_orders - pre_orders) / pre_orders * 100) if pre_orders > 0 else 0,
                        'Pre_Sales': pre_sales,
                        'Post_Sales': post_sales,
                        'Sales_Delta': post_sales - pre_sales,
                        'Sales_Delta_Percent': ((post_sales - pre_sales) / pre_sales * 100) if pre_sales > 0 else 0,
                        'Pre_ROI': pre_roi,
                        'Post_ROI': post_roi,
                        'ROI_Delta': post_roi - pre_roi
                    })
                
                marketing_df = pd.DataFrame(marketing_comparison)
                marketing_df.to_excel(writer, sheet_name='Marketing_Campaign_Comparison', index=False)
            
            # Store Performance Comparison Sheet
            if 'pre_performance' in store_analysis and 'post_performance' in store_analysis:
                pre_stores = store_analysis['pre_performance']
                post_stores = store_analysis['post_performance']
                
                # Merge pre and post performance
                store_comparison = pre_stores.merge(
                    post_stores, 
                    left_index=True, 
                    right_index=True, 
                    suffixes=('_Pre', '_Post')
                )
                
                # Calculate deltas
                for metric in ['Gross Sales', 'Total Delivered or Picked Up Orders', 'AOV', 'Net_Revenue']:
                    pre_col = f'{metric}_Pre'
                    post_col = f'{metric}_Post'
                    if pre_col in store_comparison.columns and post_col in store_comparison.columns:
                        store_comparison[f'{metric}_Delta'] = store_comparison[post_col] - store_comparison[pre_col]
                        store_comparison[f'{metric}_Delta_Percent'] = (
                            (store_comparison[post_col] - store_comparison[pre_col]) / 
                            store_comparison[pre_col] * 100
                        )
                
                store_comparison.to_excel(writer, sheet_name='Store_Performance_Comparison')
            
            # Year-over-Year Analysis Sheet with Growth Details
            yoy_data = []
            
            # Process each period (pre_todc, post_todc, overall)
            for period_name, period_data in yoy_analysis.items():
                if isinstance(period_data, dict):
                    for category, data in period_data.items():
                        if isinstance(data, dict):
                            orders_2024 = data.get('orders_2024', 0)
                            orders_2025 = data.get('orders_2025', 0)
                            sales_2024 = data.get('sales_2024', 0)
                            sales_2025 = data.get('sales_2025', 0)
                            orders_growth = data.get('orders_growth', 0)
                            sales_growth = data.get('sales_growth', 0)
                            
                            yoy_data.append({
                                'Period': period_name.replace('_', ' ').title(),
                                'Category': category.replace('_yoy', '').title(),
                                'Orders_2024': orders_2024,
                                'Orders_2025': orders_2025,
                                'Orders_Delta': orders_2025 - orders_2024,
                                'Orders_Growth_Percent': orders_growth,
                                'Sales_2024': sales_2024,
                                'Sales_2025': sales_2025,
                                'Sales_Delta': sales_2025 - sales_2024,
                                'Sales_Growth_Percent': sales_growth
                            })
            
            yoy_df = pd.DataFrame(yoy_data)
            yoy_df.to_excel(writer, sheet_name='Year_Over_Year_Analysis', index=False)
            
            # Marketing ROI Analysis Sheet
            if 'roi_analysis' in marketing_analysis:
                roi_data = marketing_analysis['roi_analysis']
                roi_df = pd.DataFrame([{
                    'Metric': 'Pre-TODC ROI',
                    'Value': roi_data.get('pre_todc_roi', 0),
                    'Unit': '%'
                }, {
                    'Metric': 'Post-TODC ROI',
                    'Value': roi_data.get('post_todc_roi', 0),
                    'Unit': '%'
                }, {
                    'Metric': 'ROI Improvement',
                    'Value': roi_data.get('roi_improvement', 0),
                    'Unit': '%'
                }, {
                    'Metric': 'Pre-TODC Total Cost',
                    'Value': roi_data.get('pre_total_cost', 0),
                    'Unit': '$'
                }, {
                    'Metric': 'Post-TODC Total Cost',
                    'Value': roi_data.get('post_total_cost', 0),
                    'Unit': '$'
                }, {
                    'Metric': 'Pre-TODC Total Sales',
                    'Value': roi_data.get('pre_total_sales', 0),
                    'Unit': '$'
                }, {
                    'Metric': 'Post-TODC Total Sales',
                    'Value': roi_data.get('post_total_sales', 0),
                    'Unit': '$'
                }])
                roi_df.to_excel(writer, sheet_name='Marketing_ROI_Analysis', index=False)
            
            # Comprehensive Pre vs Post Analysis Sheet
            if comprehensive_analysis and 'pre_todc' in comprehensive_analysis and 'post_todc' in comprehensive_analysis:
                pre_comp_metrics = comprehensive_analysis['pre_todc']
                post_comp_metrics = comprehensive_analysis['post_todc']
                comp_growth_metrics = comprehensive_analysis.get('growth', {})
                
                # Create comprehensive comparison DataFrame
                comprehensive_comparison = []
                for key in pre_comp_metrics:
                    if key == 'period':
                        continue
                    if isinstance(pre_comp_metrics[key], (int, float)):
                        pre_val = pre_comp_metrics[key]
                        post_val = post_comp_metrics[key]
                        delta = post_val - pre_val
                        delta_percent = (delta / pre_val * 100) if pre_val != 0 else 0
                        
                        comprehensive_comparison.append({
                            'Metric': key.replace('_', ' ').title(),
                            'Pre_TODC': pre_val,
                            'Post_TODC': post_val,
                            'Delta': delta,
                            'Delta_Percent': delta_percent
                        })
                
                comprehensive_df = pd.DataFrame(comprehensive_comparison)
                comprehensive_df.to_excel(writer, sheet_name='Comprehensive_Pre_Post_Analysis', index=False)
            
            # Weekly Analysis Sheet
            if weekly_data:
                weekly_df = pd.DataFrame(weekly_data)
                weekly_df.to_excel(writer, sheet_name='Weekly_Analysis', index=False)
            
            # Self-Serve Campaigns Budget vs Sales Analysis Sheet
            if self_serve_analysis:
                # Summary comparison sheet
                summary_data = []
                for year in [2024, 2025]:
                    summary_key = f'summary_{year}'
                    if summary_key in self_serve_analysis:
                        summary_data.append({
                            'Year': year,
                            'Total_Campaigns': self_serve_analysis[summary_key]['total_campaigns'],
                            'Total_Orders': self_serve_analysis[summary_key]['total_orders'],
                            'Total_Sales': self_serve_analysis[summary_key]['total_sales'],
                            'Total_Budget': self_serve_analysis[summary_key]['total_budget'],
                            'Avg_ROAS': self_serve_analysis[summary_key]['avg_roas'],
                            'Unique_Campaigns': self_serve_analysis[summary_key]['unique_campaigns']
                        })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Self_Serve_Summary', index=False)
                
                # Detailed campaigns analysis (2025)
                if 'detailed_campaigns_2025' in self_serve_analysis and len(self_serve_analysis['detailed_campaigns_2025']) > 0:
                    detailed_df = self_serve_analysis['detailed_campaigns_2025'].reset_index()
                    detailed_df.to_excel(writer, sheet_name='Self_Serve_Campaigns_2025', index=False)
                
                # Year-over-year growth analysis
                if 'yoy_growth' in self_serve_analysis:
                    yoy_data = []
                    for key, value in self_serve_analysis['yoy_growth'].items():
                        if 'delta_percent' in key:
                            metric_name = key.replace('_delta_percent', '').replace('_', ' ').title()
                            delta_value = self_serve_analysis['yoy_growth'][key.replace('_delta_percent', '_delta')]
                            yoy_data.append({
                                'Metric': metric_name,
                                'Delta': delta_value,
                                'Delta_Percent': value
                            })
                    
                    yoy_df = pd.DataFrame(yoy_data)
                    yoy_df.to_excel(writer, sheet_name='Self_Serve_YoY_Growth', index=False)
            
            # Store-Level Metrics Analysis Sheet
            if store_level_analysis and 'comparison_table' in store_level_analysis:
                store_comparison = store_level_analysis['comparison_table']
                store_comparison.to_excel(writer, sheet_name='Store_Level_Metrics', index=True)
                
                # Also create a summary sheet with totals
                summary_data = []
                metrics = ['Overall_Sales', 'Marketing_Driven_Sales', 'Organic_Sales', 'Marketing_Cost', 'Net_Payout']
                periods = ['Pre', 'Post', 'Delta', 'Delta_Percent']
                
                for metric in metrics:
                    for period in periods:
                        if period in ['Pre', 'Post', 'Delta']:
                            total = store_comparison[(period, metric)].sum()
                            summary_data.append({
                                'Metric': metric,
                                'Period': period,
                                'Total': total,
                                'Unit': '$'
                            })
                        else:  # Delta_Percent
                            pre_total = store_comparison[('Pre', metric)].sum()
                            delta_total = store_comparison[('Delta', metric)].sum()
                            total_percent = (delta_total / pre_total * 100) if pre_total != 0 else 0
                            summary_data.append({
                                'Metric': metric,
                                'Period': period,
                                'Total': total_percent,
                                'Unit': '%'
                            })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Store_Level_Summary', index=False)
            
            # Insights and Recommendations Sheet
            insights_df = pd.DataFrame({
                'Insights': insights['insights'],
                'Recommendations': insights['recommendations']
            })
            insights_df.to_excel(writer, sheet_name='Insights_Recommendations', index=False)
        
        print(f"Analysis report exported to: {filename}")
        return filename
    
    def run_complete_analysis(self):
        """Run the complete TODC impact analysis."""
        print("TODC IMPACT ANALYSIS")
        print("="*60)
        print("Analyzing the impact of TODC on DoorDash client performance...")
        print(f"Pre-TODC Period: {self.pre_todc_start} to {self.pre_todc_end}")
        print(f"Post-TODC Period: {self.post_todc_start} to {self.post_todc_end}")
        
        # Run all analyses
        financial_analysis = self.calculate_financial_metrics()
        marketing_analysis = self.analyze_marketing_campaigns()
        store_analysis = self.analyze_store_performance()
        yoy_analysis = self.year_over_year_analysis()
        
        # Run comprehensive pre vs post analysis
        comprehensive_analysis = self.analyze_comprehensive_pre_post_metrics()
        
        # Run weekly analysis
        weekly_data = self.analyze_weekly_metrics()
        
        # Run self-serve campaigns analysis
        self_serve_analysis = self.analyze_self_serve_campaigns_budget_vs_sales()
        
        # Run store-level metrics analysis
        store_level_analysis = self.analyze_store_level_metrics()
        
        # Generate insights
        insights = self.generate_insights_and_recommendations(
            financial_analysis, marketing_analysis, store_analysis, yoy_analysis
        )
        
        # Create visualizations
        self.create_visualizations(financial_analysis, marketing_analysis, store_analysis, yoy_analysis)
        
        # Export to Excel
        excel_file = self.export_to_excel(financial_analysis, marketing_analysis, store_analysis, yoy_analysis, insights, comprehensive_analysis, weekly_data, self_serve_analysis, store_level_analysis)
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE!")
        print("="*60)
        print(f"Results exported to: {excel_file}")
        
        return {
            'financial': financial_analysis,
            'marketing': marketing_analysis,
            'store': store_analysis,
            'yoy': yoy_analysis,
            'comprehensive': comprehensive_analysis,
            'weekly': weekly_data,
            'self_serve': self_serve_analysis,
            'store_level': store_level_analysis,
            'insights': insights,
            'excel_file': excel_file
        }

def main():
    """Main function to run the TODC analysis."""
    try:
        # Initialize analyzer
        analyzer = TODCAnalyzer()
        
        # Run complete analysis
        results = analyzer.run_complete_analysis()
        
        print("\nAnalysis completed successfully!")
        print("Check the generated Excel file for detailed results.")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
