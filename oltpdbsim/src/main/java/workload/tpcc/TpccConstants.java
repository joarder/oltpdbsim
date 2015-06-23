/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.workload.tpcc;

public abstract class TpccConstants {
	public static final String TBL_WAREHOUSE = "Warehouse";
    public static final String TBL_ITEM	= "Item";
    public static final String TBL_DISTRICT	= "District";
    public static final String TBL_STOCK = "Stock";
    public static final String TBL_CUSTOMER	= "Customer";
    public static final String TBL_HISTORY = "History";
    public static final String TBL_ORDERS = "Orders";
    public static final String TBL_NEW_ORDER = "New-Order";
    public static final String TBL_ORDER_LINE = "Order-Line";

    public static double SCALE_FACTOR = 1;
    public static int NUM_WAREHOUSES = 1;
    
    public static final double ZIPF_EXP = 2.0; // 1.75
    
    public static final int NUM_ITEMS = 100000;
    public static final int DISTRICTS_PER_WAREHOUSE = 10;
    public static final int STOCKS_PER_WAREHOUSE = 100000;
    public static final int CUSTOMERS_PER_DISTRICT = 3000;
    
	public static final int NUM_MOST_POPULAR_ITEM_FROM_STOCK = 10;
}