import configparser

import psycopg2

from sql_queries import optimization_queries




def optim_stats(cur, conn):
    """Prints information about the fact and dimension tables to better design a dist and sort
    key strategy."""
    for query in optimization_queries:
        print("\n\n")
        cur.execute(query)        
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()
            

            
            
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    optim_stats(cur, conn)
    
    conn.close()
    
    
    
    
if __name__=="__main__":
    main()
