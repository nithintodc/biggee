#!/usr/bin/env python3
"""
August 2025 Store-wise Analysis Script
=====================================

This script analyzes store-wise performance data for August 1-31, 2025.
It pulls all relevant data store-wise and exports to a separate Excel file.

Author: TODC Analytics Team
Date: 2025-09-29

Analysis Period: August 1, 2025 - August 31, 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import os
warnings.filterwarnings('ignore')

class AugustAnalyzer:
    def __init__(self):
        """Initialize the August Analyzer with data paths and analysis period."""
        self.data_paths = {
            'financial_2025': 'financial_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z/FINANCIAL_DETAILED_TRANSACTIONS_2025-05-09_2025-09-08_aPoMn_2025-09-27T16-55-21Z.csv',
            'marketing_2025': 'marketing_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z/MARKETING_PROMOTION_2025-05-09_2025-09-08_3tRPN_2025-09-27T16-59-12Z.csv',
            'sales_2025': 'SALES_viewByTime_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z/SALES_viewByTime_byStore_2025-05-09_2025-09-08_05SgI_2025-09-27T16-58-43Z.csv'
        }
        
        # Define August analysis period
        self.august_start = '2025-08-01'
        self.august_end = '2025-08-31'
        
        # Load all data
        self.load_data()
        
    def load_data(self):
        """Load all CSV files and prepare them for analysis."""
        print("Loading data files for August analysis...")
        
        # Load financial data
        self.financial_2025 = pd.read_csv(self.data_paths['financial_2025'])
        
        # Load marketing data
        self.marketing_2025 = pd.read_csv(self.data_paths['marketing_2025'])
        
        # Load sales data
        self.sales_2025 = pd.read_csv(self.data_paths['sales_2025'])
        
        # Process date columns
        self.process_dates()
        
        print("Data loaded successfully!")
        print(f"Financial 2025: {len(self.financial_2025):,} records")
        print(f"Marketing 2025: {len(self.marketing_2025):,} records")
        print(f"Sales 2025: {len(self.sales_2025):,} records")
        
    def process_dates(self):
        """Process and standardize date columns across all datasets."""
        # Financial data - use 'Timestamp UTC date'
        if 'Timestamp UTC date' in self.financial_2025.columns:
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Timestamp UTC date'])
        elif 'Payout date' in self.financial_2025.columns:
            self.financial_2025['date'] = pd.to_datetime(self.financial_2025['Payout date'])
        
        # Marketing data - use 'Date'
        self.marketing_2025['date'] = pd.to_datetime(self.marketing_2025['Date'])
        
        # Sales data - use 'Start Date'
        self.sales_2025['date'] = pd.to_datetime(self.sales_2025['Start Date'])
        
    def filter_by_period(self, df, start_date, end_date, date_col='date'):
        """Filter dataframe by date range."""
        return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    
    def analyze_august_financial_data(self):
        """Analyze financial data for August 1-31, 2025."""
        print("\n" + "="*60)
        print("AUGUST 2025 FINANCIAL ANALYSIS")
        print("="*60)
        
        # Filter financial data for August
        august_financial = self.filter_by_period(self.financial_2025, self.august_start, self.august_end)
        
        # Filter only successful orders
        august_orders = august_financial[august_financial['Transaction type'] == 'Order']
        
        if len(august_orders) == 0:
            print("No order data found for August 2025")
            return {}
        
        # Handle different column name formats
        marketing_fees_col = None
        customer_discounts_col = None
        dd_discounts_col = None
        
        # Check for column names
        if 'Marketing fees | (including any applicable taxes)' in august_orders.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in august_orders.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        if 'Customer discounts from marketing | (funded by you)' in august_orders.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        elif 'Customer discounts from marketing | (Funded by you)' in august_orders.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        
        if 'Customer discounts from marketing | (funded by DoorDash)' in august_orders.columns:
            dd_discounts_col = 'Customer discounts from marketing | (funded by DoorDash)'
        elif 'Customer discounts from marketing | (Funded by DoorDash)' in august_orders.columns:
            dd_discounts_col = 'Customer discounts from marketing | (Funded by DoorDash)'
        
        # Calculate store-wise financial metrics
        store_financial_metrics = august_orders.groupby('Store ID').agg({
            'Subtotal': 'sum',
            'Commission': lambda x: x.abs().sum(),  # Commission is negative
            'Net total': 'sum',
            'Transaction type': 'count'  # Count of orders
        }).round(2)
        
        # Add marketing fees and customer discounts by store
        if marketing_fees_col:
            store_marketing_fees = august_orders.groupby('Store ID')[marketing_fees_col].sum()
            store_financial_metrics['Marketing_Fees'] = store_marketing_fees
        
        if customer_discounts_col:
            store_customer_discounts = august_orders.groupby('Store ID')[customer_discounts_col].sum()
            store_financial_metrics['Customer_Discounts_Funded_by_You'] = store_customer_discounts
        
        if dd_discounts_col:
            store_dd_discounts = august_orders.groupby('Store ID')[dd_discounts_col].sum()
            store_financial_metrics['Customer_Discounts_Funded_by_DD'] = store_dd_discounts
        
        # Calculate additional metrics
        store_financial_metrics['Avg_Order_Value'] = (
            store_financial_metrics['Subtotal'] / store_financial_metrics['Transaction type']
        ).round(2)
        
        store_financial_metrics['Commission_Rate'] = (
            (store_financial_metrics['Commission'] / store_financial_metrics['Subtotal']) * 100
        ).round(2)
        
        if marketing_fees_col:
            store_financial_metrics['Marketing_Fee_Rate'] = (
                (store_financial_metrics['Marketing_Fees'] / store_financial_metrics['Subtotal']) * 100
            ).round(2)
        
        # Rename columns for clarity
        store_financial_metrics = store_financial_metrics.rename(columns={
            'Subtotal': 'Total_Sales',
            'Commission': 'Total_Commission',
            'Net total': 'Net_Payout',
            'Transaction type': 'Total_Orders'
        })
        
        print(f"August Financial Analysis Summary:")
        print(f"  Total Stores: {len(store_financial_metrics)}")
        print(f"  Total Orders: {store_financial_metrics['Total_Orders'].sum():,}")
        print(f"  Total Sales: ${store_financial_metrics['Total_Sales'].sum():,.2f}")
        print(f"  Total Net Payout: ${store_financial_metrics['Net_Payout'].sum():,.2f}")
        
        return store_financial_metrics
    
    def analyze_august_marketing_data(self):
        """Analyze marketing data for August 1-31, 2025."""
        print("\n" + "="*60)
        print("AUGUST 2025 MARKETING ANALYSIS")
        print("="*60)
        
        # Filter marketing data for August
        august_marketing = self.filter_by_period(self.marketing_2025, self.august_start, self.august_end)
        
        if len(august_marketing) == 0:
            print("No marketing data found for August 2025")
            return {}
        
        # Handle different column name formats
        customer_discounts_col = None
        marketing_fees_col = None
        
        if 'Customer discounts from marketing | (Funded by you)' in august_marketing.columns:
            customer_discounts_col = 'Customer discounts from marketing | (Funded by you)'
        elif 'Customer discounts from marketing | (funded by you)' in august_marketing.columns:
            customer_discounts_col = 'Customer discounts from marketing | (funded by you)'
        
        if 'Marketing fees | (including any applicable taxes)' in august_marketing.columns:
            marketing_fees_col = 'Marketing fees | (including any applicable taxes)'
        elif 'Marketing fees (for historical reference only) | (all discounts and fees)' in august_marketing.columns:
            marketing_fees_col = 'Marketing fees (for historical reference only) | (all discounts and fees)'
        
        # Calculate store-wise marketing metrics
        agg_dict = {
            'Orders': 'sum',
            'Sales': 'sum',
            'ROAS': 'mean'
        }
        
        if customer_discounts_col:
            agg_dict[customer_discounts_col] = 'sum'
        if marketing_fees_col:
            agg_dict[marketing_fees_col] = 'sum'
        
        store_marketing_metrics = august_marketing.groupby('Store ID').agg(agg_dict).round(2)
        
        # Calculate total marketing cost
        if customer_discounts_col and marketing_fees_col:
            store_marketing_metrics['Total_Marketing_Cost'] = (
                store_marketing_metrics[customer_discounts_col] + 
                store_marketing_metrics[marketing_fees_col]
            ).round(2)
        elif customer_discounts_col:
            store_marketing_metrics['Total_Marketing_Cost'] = store_marketing_metrics[customer_discounts_col]
        elif marketing_fees_col:
            store_marketing_metrics['Total_Marketing_Cost'] = store_marketing_metrics[marketing_fees_col]
        else:
            store_marketing_metrics['Total_Marketing_Cost'] = 0
        
        # Calculate ROI
        store_marketing_metrics['Marketing_ROI'] = (
            (store_marketing_metrics['Sales'] - store_marketing_metrics['Total_Marketing_Cost']) / 
            store_marketing_metrics['Total_Marketing_Cost'] * 100
        ).round(2)
        
        # Rename columns for clarity
        store_marketing_metrics = store_marketing_metrics.rename(columns={
            'Orders': 'Marketing_Orders',
            'Sales': 'Marketing_Sales',
            'ROAS': 'Avg_ROAS'
        })
        
        print(f"August Marketing Analysis Summary:")
        print(f"  Stores with Marketing: {len(store_marketing_metrics)}")
        print(f"  Total Marketing Orders: {store_marketing_metrics['Marketing_Orders'].sum():,}")
        print(f"  Total Marketing Sales: ${store_marketing_metrics['Marketing_Sales'].sum():,.2f}")
        print(f"  Total Marketing Cost: ${store_marketing_metrics['Total_Marketing_Cost'].sum():,.2f}")
        
        return store_marketing_metrics
    
    def analyze_august_sales_data(self):
        """Analyze sales data for August 1-31, 2025."""
        print("\n" + "="*60)
        print("AUGUST 2025 SALES ANALYSIS")
        print("="*60)
        
        # Filter sales data for August
        august_sales = self.filter_by_period(self.sales_2025, self.august_start, self.august_end)
        
        if len(august_sales) == 0:
            print("No sales data found for August 2025")
            return {}
        
        # Calculate store-wise sales metrics
        store_sales_metrics = august_sales.groupby(['Store ID', 'Store Name']).agg({
            'Gross Sales': 'sum',
            'Total Delivered or Picked Up Orders': 'sum',
            'AOV': 'mean',
            'Total Commission': 'sum'
        }).round(2)
        
        # Calculate additional metrics
        store_sales_metrics['Net_Revenue'] = (
            store_sales_metrics['Gross Sales'] - store_sales_metrics['Total Commission']
        ).round(2)
        
        # Rename columns for clarity
        store_sales_metrics = store_sales_metrics.rename(columns={
            'Gross Sales': 'Total_Sales',
            'Total Delivered or Picked Up Orders': 'Total_Orders',
            'AOV': 'Avg_Order_Value',
            'Total Commission': 'Total_Commission'
        })
        
        print(f"August Sales Analysis Summary:")
        print(f"  Total Stores: {len(store_sales_metrics)}")
        print(f"  Total Orders: {store_sales_metrics['Total_Orders'].sum():,}")
        print(f"  Total Sales: ${store_sales_metrics['Total_Sales'].sum():,.2f}")
        print(f"  Total Net Revenue: ${store_sales_metrics['Net_Revenue'].sum():,.2f}")
        
        return store_sales_metrics
    
    def create_comprehensive_august_analysis(self):
        """Create comprehensive August analysis combining all data sources."""
        print("\n" + "="*60)
        print("COMPREHENSIVE AUGUST 2025 ANALYSIS")
        print("="*60)
        
        # Get individual analyses
        financial_metrics = self.analyze_august_financial_data()
        marketing_metrics = self.analyze_august_marketing_data()
        sales_metrics = self.analyze_august_sales_data()
        
        # Create comprehensive store analysis
        comprehensive_analysis = {}
        
        # Get all unique store IDs
        all_store_ids = set()
        if len(financial_metrics) > 0:
            all_store_ids.update(financial_metrics.index)
        if len(marketing_metrics) > 0:
            all_store_ids.update(marketing_metrics.index)
        if len(sales_metrics) > 0:
            all_store_ids.update(sales_metrics.index.get_level_values('Store ID'))
        
        # Create comprehensive dataframe
        comprehensive_data = []
        
        for store_id in sorted(all_store_ids):
            store_data = {'Store_ID': store_id}
            
            # Add financial metrics
            if store_id in financial_metrics.index:
                fin_data = financial_metrics.loc[store_id]
                store_data.update({
                    'Financial_Orders': fin_data.get('Total_Orders', 0),
                    'Financial_Sales': fin_data.get('Total_Sales', 0),
                    'Financial_Commission': fin_data.get('Total_Commission', 0),
                    'Financial_Net_Payout': fin_data.get('Net_Payout', 0),
                    'Financial_Avg_Order_Value': fin_data.get('Avg_Order_Value', 0),
                    'Financial_Commission_Rate': fin_data.get('Commission_Rate', 0),
                    'Financial_Marketing_Fees': fin_data.get('Marketing_Fees', 0),
                    'Financial_Customer_Discounts_You': fin_data.get('Customer_Discounts_Funded_by_You', 0),
                    'Financial_Customer_Discounts_DD': fin_data.get('Customer_Discounts_Funded_by_DD', 0)
                })
            
            # Add marketing metrics
            if store_id in marketing_metrics.index:
                mkt_data = marketing_metrics.loc[store_id]
                store_data.update({
                    'Marketing_Orders': mkt_data.get('Marketing_Orders', 0),
                    'Marketing_Sales': mkt_data.get('Marketing_Sales', 0),
                    'Marketing_Avg_ROAS': mkt_data.get('Avg_ROAS', 0),
                    'Marketing_Total_Cost': mkt_data.get('Total_Marketing_Cost', 0),
                    'Marketing_ROI': mkt_data.get('Marketing_ROI', 0)
                })
            
            # Add sales metrics
            if store_id in sales_metrics.index.get_level_values('Store ID'):
                sales_data = sales_metrics.loc[store_id]
                # Get store name from the multi-index
                store_name = 'Unknown'
                if isinstance(sales_metrics.index, pd.MultiIndex):
                    # Find the store name for this store_id
                    store_names = sales_metrics.index[sales_metrics.index.get_level_values('Store ID') == store_id]
                    if len(store_names) > 0:
                        store_name = store_names[0][1]  # Get the Store Name from the tuple
                
                store_data.update({
                    'Sales_Total_Sales': sales_data.get('Total_Sales', 0),
                    'Sales_Total_Orders': sales_data.get('Total_Orders', 0),
                    'Sales_Avg_Order_Value': sales_data.get('Avg_Order_Value', 0),
                    'Sales_Total_Commission': sales_data.get('Total_Commission', 0),
                    'Sales_Net_Revenue': sales_data.get('Net_Revenue', 0),
                    'Store_Name': store_name
                })
            
            # Calculate derived metrics
            financial_sales = store_data.get('Financial_Sales', 0)
            sales_total_sales = store_data.get('Sales_Total_Sales', 0)
            
            # Handle pandas Series values
            if hasattr(financial_sales, 'iloc'):
                financial_sales = financial_sales.iloc[0] if len(financial_sales) > 0 else 0
            if hasattr(sales_total_sales, 'iloc'):
                sales_total_sales = sales_total_sales.iloc[0] if len(sales_total_sales) > 0 else 0
            
            total_sales = max(financial_sales, sales_total_sales)
            marketing_sales = store_data.get('Marketing_Sales', 0)
            
            # Handle pandas Series values for marketing_sales
            if hasattr(marketing_sales, 'iloc'):
                marketing_sales = marketing_sales.iloc[0] if len(marketing_sales) > 0 else 0
            
            organic_sales = total_sales - marketing_sales if total_sales > 0 else 0
            
            store_data.update({
                'Total_Sales': total_sales,
                'Organic_Sales': organic_sales,
                'Marketing_Sales': marketing_sales,
                'Organic_Percentage': (organic_sales / total_sales * 100) if total_sales > 0 else 0,
                'Marketing_Percentage': (marketing_sales / total_sales * 100) if total_sales > 0 else 0
            })
            
            comprehensive_data.append(store_data)
        
        comprehensive_df = pd.DataFrame(comprehensive_data)
        comprehensive_df = comprehensive_df.fillna(0)
        
        # Sort by total sales descending
        comprehensive_df = comprehensive_df.sort_values('Total_Sales', ascending=False)
        
        print(f"\nComprehensive August Analysis Summary:")
        print(f"  Total Stores Analyzed: {len(comprehensive_df)}")
        print(f"  Total Sales: ${comprehensive_df['Total_Sales'].sum():,.2f}")
        print(f"  Total Marketing Sales: ${comprehensive_df['Marketing_Sales'].sum():,.2f}")
        print(f"  Total Organic Sales: ${comprehensive_df['Organic_Sales'].sum():,.2f}")
        print(f"  Total Marketing Cost: ${comprehensive_df['Marketing_Total_Cost'].sum():,.2f}")
        
        return {
            'comprehensive': comprehensive_df,
            'financial': financial_metrics,
            'marketing': marketing_metrics,
            'sales': sales_metrics
        }
    
    def export_to_excel(self, analysis_results):
        """Export August analysis results to Excel file."""
        print("\n" + "="*60)
        print("EXPORTING AUGUST ANALYSIS TO EXCEL")
        print("="*60)
        
        filename = f"August_2025_Store_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Comprehensive Analysis Sheet
            comprehensive_df = analysis_results['comprehensive']
            comprehensive_df.to_excel(writer, sheet_name='Comprehensive_Analysis', index=False)
            
            # Financial Analysis Sheet
            if len(analysis_results['financial']) > 0:
                financial_df = analysis_results['financial'].reset_index()
                financial_df.to_excel(writer, sheet_name='Financial_Analysis', index=False)
            
            # Marketing Analysis Sheet
            if len(analysis_results['marketing']) > 0:
                marketing_df = analysis_results['marketing'].reset_index()
                marketing_df.to_excel(writer, sheet_name='Marketing_Analysis', index=False)
            
            # Sales Analysis Sheet
            if len(analysis_results['sales']) > 0:
                sales_df = analysis_results['sales'].reset_index()
                sales_df.to_excel(writer, sheet_name='Sales_Analysis', index=False)
            
            # Summary Statistics Sheet
            summary_data = []
            
            # Overall summary
            summary_data.append({
                'Metric': 'Total Stores',
                'Value': len(comprehensive_df),
                'Unit': 'Stores'
            })
            summary_data.append({
                'Metric': 'Total Sales',
                'Value': comprehensive_df['Total_Sales'].sum(),
                'Unit': '$'
            })
            summary_data.append({
                'Metric': 'Total Marketing Sales',
                'Value': comprehensive_df['Marketing_Sales'].sum(),
                'Unit': '$'
            })
            summary_data.append({
                'Metric': 'Total Organic Sales',
                'Value': comprehensive_df['Organic_Sales'].sum(),
                'Unit': '$'
            })
            summary_data.append({
                'Metric': 'Total Marketing Cost',
                'Value': comprehensive_df['Marketing_Total_Cost'].sum(),
                'Unit': '$'
            })
            # Calculate average marketing ROI (handle non-numeric values)
            marketing_roi_values = pd.to_numeric(comprehensive_df['Marketing_ROI'], errors='coerce')
            avg_marketing_roi = marketing_roi_values.mean() if not marketing_roi_values.isna().all() else 0
            
            summary_data.append({
                'Metric': 'Average Marketing ROI',
                'Value': avg_marketing_roi,
                'Unit': '%'
            })
            
            # Calculate average order value (handle non-numeric values)
            aov_values = pd.to_numeric(comprehensive_df['Sales_Avg_Order_Value'], errors='coerce')
            avg_order_value = aov_values.mean() if not aov_values.isna().all() else 0
            
            summary_data.append({
                'Metric': 'Average Order Value',
                'Value': avg_order_value,
                'Unit': '$'
            })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary_Statistics', index=False)
            
            # Top Performing Stores Sheet
            top_stores = comprehensive_df.head(20)[
                ['Store_ID', 'Store_Name', 'Total_Sales', 'Marketing_Sales', 
                 'Organic_Sales', 'Marketing_ROI', 'Sales_Avg_Order_Value']
            ]
            top_stores.to_excel(writer, sheet_name='Top_20_Stores', index=False)
        
        print(f"August analysis exported to: {filename}")
        return filename
    
    def run_august_analysis(self):
        """Run the complete August analysis."""
        print("AUGUST 2025 STORE-WISE ANALYSIS")
        print("="*60)
        print(f"Analysis Period: {self.august_start} to {self.august_end}")
        print("Analyzing store-wise performance for August 2025...")
        
        # Run comprehensive analysis
        analysis_results = self.create_comprehensive_august_analysis()
        
        # Export to Excel
        excel_file = self.export_to_excel(analysis_results)
        
        print("\n" + "="*60)
        print("AUGUST ANALYSIS COMPLETE!")
        print("="*60)
        print(f"Results exported to: {excel_file}")
        
        return {
            'analysis_results': analysis_results,
            'excel_file': excel_file
        }

def main():
    """Main function to run the August analysis."""
    try:
        # Initialize analyzer
        analyzer = AugustAnalyzer()
        
        # Run August analysis
        results = analyzer.run_august_analysis()
        
        print("\nAugust analysis completed successfully!")
        print("Check the generated Excel file for detailed store-wise results.")
        
    except Exception as e:
        print(f"Error during August analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
