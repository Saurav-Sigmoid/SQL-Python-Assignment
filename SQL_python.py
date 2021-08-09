import psycopg2
import pandas as pd

from config import config
import logging

logging.basicConfig(level=logging.INFO)


#All Table and insert are done in pgadmin only query is in the this file


# Solution for problem statement1
def solution1(cur):
    # Assumed that not give query if manager is not present if otherwise use the below statement
    # cur.execute("select e.empno,e.ename,m.ename from emp as e left join emp as m on e.mgr=m.empno")
    cur.execute("select e.empno,e.ename,m.ename from emp as e inner join emp as m on e.mgr=m.empno")
    df = pd.DataFrame(cur.fetchall())
    df.to_excel('Soluion1.xlsx', header=["E_Id", "E_Name", "M_Name"], index=False)


# Solution for problem statement2
def solution2(cur):
    # Solution for the given table for the current date as end date was not specified
    cur.execute(
        "select e.empno,e.ename,d.dname,comms.comm*comms.mon,comms.mon "
        "from (select empno,Cast(((Cast (Current_date-min(startdate) as float))/365.0*12) as Int) as mon,"
        ""
        "sum(CASE when comm is not null then comm else 0 end) comm from jobhist group by(empno)) comms "
        "inner join emp e on e.empno=comms.empno inner join dept d on d.deptno=e.deptno")
    df2 = pd.DataFrame(cur.fetchall())
    df2.to_excel('compensation.xlsx',
                 header=["Emp Name", "Emp No", "Dept Name", "Total Compensation", "Months Spent in Organization"],
                 index=False)
    # Also saving into the csv format as it pgadmin does not have an excel import statement
    df2.to_csv('c.csv',
               header=["Emp Name", "Emp No", "Dept Name", "Total Compensation", "Months Spent in Organization"],
               index=False)

# Solution for problem statement3
def solution3(cur):
    #Table is create in the pgadmin folder as commpen
    cur.execute("COPY commpen FROM '/Users/sauravvarma/PycharmProjects/pythonProject/Assignments/c.csv' "
                "DELIMITER ',' CSV HEADER;")

# Solution for problem statement4
def solution4(cur):
    cur.execute(
        "select d.deptno,d.dname,Case when e.c is null then 0 else e.c end from "
        "(select deptname,sum(comm)as c from commpen group by(deptname)) e right join dept d on d.dname=e.deptname")
    df3 = pd.DataFrame(cur.fetchall())
    df3.to_excel('deptcomm.xlsx', header=["Dept No", "Dept Name", "Compensation"], index=False)

# Connection of Database
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        solution1(cur)

        solution2(cur)

        solution3(cur)

        solution4(cur)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            return 2


if __name__ == '__main__':
    connect()
