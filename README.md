# Sql DB Client

Provides additional functionalities to work with DBMS utilizing mainly 2 powerful packages sqlalchemy and pandas.

The main goal to replace basic SQL client software application (e.g. [DBeaver](https://en.wikipedia.org/wiki/DBeaver)).
Especially helpful for people who are used to work with pandas and its powerful abilities in terms of analytical research.
Designed mainly to use alongside with Jupyter Notebook (i.e. GUI environment but endless abilities of Python and its libraries).

Basically, it is a wrapper around pd.DataFrame().to_sql function and sqlalchemy functionalities such as transaction management
with the following features:
- detailed logging with query execution time collecting (also can be saved to file to keep track of your every action as long as need)
- easy transaction management 
        
        with sql_executor:
            sql_executor.execute_query('CREATE VIEW temp_view AS SELECT 1 AS num')
            temp_view = sql_executor.read_query('SELECT * FROM temp_view')
            ...
- query results management using SQLLite database (i.e. a file near your Python script)
  - UUID for each query run (to easily get any executed query result; to work with one database from different scripts with no need to synchronization)
  - query start and finish time, duration
  - result (a pandas DataFrame for read query)
  
        You do not need to save your select results into csv and excel in order to look at them in the future.
        They will be avaiable as long as you need in your own file-based database.
  
- (optional) pandas DataFrame enhancements in Jupyter (such as displaying DataFrame with all rows and columns and full colwidth by simply calling its one method)
- query pre-processing (e.g. automatically adding set LIMIT value to the end of every select query in order to prevent memory overuse)
- async query execution, you can easily run your query in background

      await sql_async_planner.plan_execution('SELECT 1 AS a')
      result = await sql_async_planner.get_result()
- 