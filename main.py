import psycopg2

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS public.accounts( " +
                    "customer_id bigint NOT NULL," +
                    "first_name character varying(255)," +
                    "last_name character varying(255)," +
                    "address_1 character varying(255)," +
                    "address_2 character varying(255)," +
                    "city character varying(255)," +
                    "state character varying(255)," +
                    "zip_code character varying(255)," +
                    "join_date date," +
                    "PRIMARY KEY (customer_id))")

    cursor.execute("CREATE TABLE IF NOT EXISTS public.products( " +
                   "product_id bigint NOT NULL," +
                   "product_code character varying(255)," +
                   "product_description character varying(255)," +
                    "PRIMARY KEY (product_id))")

    cursor.execute("CREATE TABLE IF NOT EXISTS public.transactions( " +
                    "transaction_id character varying(255) NOT NULL," +
                    "transaction_date date," +
                    "product_id bigint," +
                    "quantity bigint," +
                    "account_id bigint," +
                    "FOREIGN KEY(product_id) REFERENCES public.products(product_id)," +
                    "FOREIGN KEY(account_id) REFERENCES public.accounts(customer_id))")

    conn.commit()

    # copy from accounts.csv to accounts table
    with open("data/accounts.csv", 'r') as f:
        cursor.copy_expert("COPY accounts(customer_id,first_name,last_name,address_1,address_2,city,state,zip_code,join_date) "
                           "FROM stdin WITH CSV HEADER DELIMITER AS ','", f)
        conn.commit()

    # copy from products.csv to products table
    with open("data/products.csv", 'r') as f:
        cursor.copy_expert("COPY products(product_id,product_code,product_description) "
                           "FROM stdin WITH CSV HEADER DELIMITER AS ','", f)
        conn.commit()

    # copy from transactions.csv to transactions table
    # this process creates a temporary table and then copies data (with correct foreign keys) from it to transactions table
    # the csv file itself has columns that can only be present during SELECT * FROM (product_code, product_description)
    # I only took ones that can actually be considered foreign keys (product_id, account_id)
    cursor.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temptable( " +
                    "transaction_id character varying(255) NOT NULL," +
                    "transaction_date date," +
                    "product_id bigint," +
                    "product_code character varying(255)," +
                    "product_description character varying(255)," +
                    "quantity bigint," +
                    "account_id bigint)")
    conn.commit()


    with open("data/transactions.csv", 'r') as f:
        cursor.copy_expert("COPY temptable(transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id) FROM stdin WITH CSV HEADER DELIMITER AS ','", f)
        conn.commit()

    cursor.execute("INSERT INTO public.transactions(transaction_id, transaction_date, product_id, quantity, account_id) "
                   "SELECT transaction_id, transaction_date, product_id, quantity, account_id FROM temptable")
    cursor.execute("DROP TABLE temptable")
    conn.commit()

    conn.close()
    pass

if __name__ == "__main__":
    main()
