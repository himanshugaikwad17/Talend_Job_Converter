#!/usr/bin/env python3
"""
Test script for SQL injection functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_malformed_dag_detection():
    """Test detection of malformed DAG patterns"""
    print("=== Testing Malformed DAG Detection ===")
    
    # Test case 1: Malformed DAG with truncated SQL
    malformed_dag = '''
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DAG_ID = "talend_test_0_1"
DAG_DESCRIPTION = "Talend job Test_0.1"

DEFAULT_ARGS = {
    "start_date": datetime(2023, 12, 1),
}

SNOWFLAKE_CONNECTION = "SNOWFLAKE_CONN"

with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
) as dag:

    snowflake_connection_task = SnowflakeOperator(
        task_id="snowflake_connection",
        snowflake_conn_id=SNOWFLAKE_CONNECTION,
        sql="-- QUERYSTORE:QUERYSTORE_TYPE:\\nBUILT_IN",
        trigger_rule="all_done",
    )

    snowflake_row_task = SnowflakeOperator(
        task_id="snowflake_row",
        snowflake_conn_id=SNOWFLAKE_CONNECTION,
        sql="-- PROPERTIES Query:\\nMERGE INTO RETAIL_DEV.gen2_adm_stg.stg_gen2adm_final_snap_hist_delta d\\nUSING  ",
        trigger_rule="all_done",
    )

    snowflake_connection_task >> snowflake_row_task
'''
    
    sql_details = {
        "tSnowflakeRow": {
            "sql": """-- PROPERTIES Query:
MERGE INTO  RETAIL_DEV.gen2_adm_stg.stg_gen2adm_final_snap_hist_delta d
USING  RETAIL_DEV.GEN2_ADM_STG.CXT_INT_TRANSPOSE_DETAIL_HIST h
ON h.uccontract=d.contract_num and h.end_dt=d.snapshot_date
WHEN MATCHED THEN
UPDATE SET
CXT_EYEBALL_DATE1   = EVENT_DATE1   ,
CXT_EYEBALL_DATE2   =EVENT_DATE2    ,
CXT_EYEBALL_DATE3   =EVENT_DATE3    ,
CXT_EYEBALL_DATE4   =EVENT_DATE4    ,
CXT_EYEBALL_DATE5   =EVENT_DATE5    ,
CXT_EYEBALL_DATE6   =EVENT_DATE6    ,
CXT_EYEBALL_DATE7   =EVENT_DATE7    ,
CXT_EYEBALL_DATE8   =EVENT_DATE8    ,
CXT_EYEBALL_DATE9   =EVENT_DATE9    ,
CXT_EYEBALL_DATE10  =EVENT_DATE10   ,
CXT_EYEBALL_DATE11  =EVENT_DATE11   ,
CXT_EYEBALL_DATE12  =EVENT_DATE12   ,
CXT_EYEBALL_DATE13  =EVENT_DATE13   ,
CXT_EYEBALL_DATE14  =EVENT_DATE14   ,
CXT_EYEBALL_DATE15  =EVENT_DATE15   ,
CXT_EYEBALL_DATE16  =EVENT_DATE16   ,
CXT_EYEBALL_DATE17  =EVENT_DATE17   ,
CXT_EYEBALL_DATE18  =EVENT_DATE18   ,
CXT_EYEBALL_DATE19  =EVENT_DATE19   ,
CXT_EYEBALL_DATE20  =EVENT_DATE20   ,
CXT_EYEBALL_KEY1    =KEY1           ,
CXT_EYEBALL_KEY2    =KEY2           ,
CXT_EYEBALL_KEY3    =KEY3           ,
CXT_EYEBALL_KEY4    =KEY4           ,
CXT_EYEBALL_KEY5    =KEY5           ,
CXT_EYEBALL_KEY6    =KEY6           ,
CXT_EYEBALL_KEY7    =KEY7           ,
CXT_EYEBALL_KEY8    =KEY8           ,
CXT_EYEBALL_KEY9    =KEY9           ,
CXT_EYEBALL_KEY10   =KEY10          ,
CXT_EYEBALL_KEY11   =KEY11          ,
CXT_EYEBALL_KEY12   =KEY12          ,
CXT_EYEBALL_KEY13   =KEY13          ,
CXT_EYEBALL_KEY14   =KEY14          ,
CXT_EYEBALL_KEY15   =KEY15          ,
CXT_EYEBALL_KEY16   =KEY16          ,
CXT_EYEBALL_KEY17   =KEY17          ,
CXT_EYEBALL_KEY18   =KEY18          ,
CXT_EYEBALL_KEY19   =KEY19          ,
CXT_EYEBALL_KEY20   =KEY20          ,
CXT_EYEBALL_VALUE1  =VALUE1         ,
CXT_EYEBALL_VALUE2  =VALUE2         ,
CXT_EYEBALL_VALUE3  =VALUE3         ,
CXT_EYEBALL_VALUE4  =VALUE4         ,
CXT_EYEBALL_VALUE5  =VALUE5         ,
CXT_EYEBALL_VALUE6  =VALUE6         ,
CXT_EYEBALL_VALUE7  =VALUE7         ,
CXT_EYEBALL_VALUE8  =VALUE8         ,
CXT_EYEBALL_VALUE9  =VALUE9         ,
CXT_EYEBALL_VALUE10 =VALUE10        ,
CXT_EYEBALL_VALUE11 =VALUE11        ,
CXT_EYEBALL_VALUE12 =VALUE12        ,
CXT_EYEBALL_VALUE13 =VALUE13        ,
CXT_EYEBALL_VALUE14 =VALUE14        ,
CXT_EYEBALL_VALUE15 =VALUE15        ,
CXT_EYEBALL_VALUE16 =VALUE16        ,
CXT_EYEBALL_VALUE17 =VALUE17        ,
CXT_EYEBALL_VALUE18 =VALUE18        ,
CXT_EYEBALL_VALUE19 =VALUE19        ,
CXT_EYEBALL_VALUE20 =VALUE20""",
            "type": "tSnowflakeRow"
        }
    }
    
    print("Input DAG (malformed):")
    print(malformed_dag)
    print("\n" + "="*80 + "\n")
    
    # Import the function after setting up the path
    from app import inject_sql_into_dag_tasks
    
    # Test the injection
    result = inject_sql_into_dag_tasks(malformed_dag, sql_details)
    
    print("Output DAG (fixed):")
    print(result)
    print("\n" + "="*80 + "\n")
    
    # Verify the result
    if "USING  '''" in result:
        print("❌ ERROR: Malformed SQL still present in output")
        return False
    elif "MERGE INTO" in result and "USING" in result and "WHEN MATCHED" in result:
        print("✅ SUCCESS: DAG properly rebuilt with complete SQL")
        return True
    else:
        print("❌ ERROR: Unexpected output format")
        return False

def test_clean_dag_injection():
    """Test SQL injection into a clean DAG"""
    print("=== Testing Clean DAG Injection ===")
    
    clean_dag = '''
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DAG_ID = "talend_test_0_1"
DAG_DESCRIPTION = "Talend job Test_0.1"

DEFAULT_ARGS = {
    "start_date": datetime(2023, 12, 1),
}

SNOWFLAKE_CONNECTION = "SNOWFLAKE_CONN"

with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
) as dag:

    snowflake_row_task = SnowflakeOperator(
        task_id="snowflake_row",
        snowflake_conn_id=SNOWFLAKE_CONNECTION,
        trigger_rule="all_done",
    )
'''
    
    sql_details = {
        "tSnowflakeRow": {
            "sql": "SELECT * FROM test_table",
            "type": "tSnowflakeRow"
        }
    }
    
    print("Input DAG (clean):")
    print(clean_dag)
    print("\n" + "="*80 + "\n")
    
    # Import the function
    from app import inject_sql_into_dag_tasks
    
    # Test the injection
    result = inject_sql_into_dag_tasks(clean_dag, sql_details)
    
    print("Output DAG (with SQL):")
    print(result)
    print("\n" + "="*80 + "\n")
    
    # Verify the result
    if "sql=" in result and "SELECT * FROM test_table" in result:
        print("✅ SUCCESS: SQL properly injected into clean DAG")
        return True
    else:
        print("❌ ERROR: SQL not properly injected")
        return False

def test_rebuild_dag_function():
    """Test the rebuild DAG function directly"""
    print("=== Testing DAG Rebuild Function ===")
    
    malformed_dag = '''
DAG_ID = "talend_test_0_1"
DAG_DESCRIPTION = "Talend job Test_0.1"
SNOWFLAKE_CONNECTION = "SNOWFLAKE_CONN"
'''
    
    sql_details = {
        "tSnowflakeRow": {
            "sql": "SELECT * FROM test_table",
            "type": "tSnowflakeRow"
        },
        "tSnowflakeConnection": {
            "sql": "CREATE TABLE test_table (id INT)",
            "type": "tSnowflakeConnection"
        }
    }
    
    print("Input DAG (minimal):")
    print(malformed_dag)
    print("\n" + "="*80 + "\n")
    
    # Import the function
    from app import rebuild_dag_with_sql
    
    # Test the rebuild
    result = rebuild_dag_with_sql(malformed_dag, sql_details)
    
    print("Output DAG (rebuilt):")
    print(result)
    print("\n" + "="*80 + "\n")
    
    # Verify the result
    if ("task_1" in result and "task_2" in result and 
        "SELECT * FROM test_table" in result and 
        "CREATE TABLE test_table" in result):
        print("✅ SUCCESS: DAG properly rebuilt with multiple tasks")
        return True
    else:
        print("❌ ERROR: DAG rebuild failed")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing SQL Injection Functionality")
    print("="*80)
    
    tests = [
        test_malformed_dag_detection,
        test_clean_dag_injection,
        test_rebuild_dag_function
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ ERROR: Test failed with exception: {e}")
        print("\n")
    
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The SQL injection functionality is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Please review the implementation.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 