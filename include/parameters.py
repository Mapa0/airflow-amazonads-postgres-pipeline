from datetime import datetime, timedelta

class Parameters:
    def set_parameters(**kwargs):
        ti = kwargs['ti']
        
        days_offset = kwargs.get('days_offset', 1) 
        analysis_window = kwargs.get('analysis_window', 7) 

        end_date = datetime.now().date() - timedelta(days=days_offset)
        start_date = end_date - timedelta(days=analysis_window)

        time_window_start = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        time_window_end = end_date.strftime("%Y-%m-%dT%H:%M:%S")

        query = kwargs.get('query', 'SELECT 0')
        table_name = kwargs.get('table_name', 'default_table')

        ti.xcom_push(key='sql_query', value=query)
        ti.xcom_push(key='table_name', value=table_name)
        ti.xcom_push(key='timeWindowStart', value=time_window_start)
        ti.xcom_push(key='timeWindowEnd', value=time_window_end)

        print(f"Time Window Start: {time_window_start}")
        print(f"Time Window End: {time_window_end}")
        print(f"Query: {query}")
        print(f"Table Name: {table_name}")