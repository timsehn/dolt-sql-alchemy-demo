from sqlalchemy import create_engine, MetaData, Table, Column, select, text

engine = create_engine(
    "mysql+mysqlconnector://root@127.0.0.1:3306/sql_alchemy_demo")

con = engine.connect()
metadata_obj = MetaData()

dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
stmt = select(dolt_log.c.commit_hash).order_by(dolt_log.c.date.desc())
results_proxy = con.execute(stmt)

for row in results_proxy.fetchall():
    commit = row[0]
    print(commit)

    # Go back in time.
    database_spec = "sql_alchemy_demo/" + commit 
    metadata_obj = MetaData(schema=database_spec)
    metadata_obj.reflect(engine)
    try: 
        employees_table = Table("employees", metadata_obj, autoload_with=engine)
        for c in employees_table.c:
            print(c)
    except:
        print("employees table not found")
        pass
