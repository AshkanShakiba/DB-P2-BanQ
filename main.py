import psycopg2


def main():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="postgrespass",
            host="localhost",
            port="5432",
            database="postgres"
        )
        cursor = connection.cursor()

        create_tables(cursor)

        cursor.close()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)


def create_tables(cursor):
    cursor.execute('''
        create table account(
            number integer not null,
            username varchar not null
                constraint account_primary_key
                    primary key,
            password varchar not null,
            first_name varchar not null,
            last_name varchar,
            national_id bigint not null,
            date_of_birth date,
            type varchar,
            interest_rate double precision default 0
        );
        
        create unique index number_unique_index
            on account(number);
            
        create table login_log(
            username varchar not null
                constraint login_log_username_foreign_key
                    references account,
            time timestamp not null,
            constraint login_log_primary_key
                primary key(username, time)
        );
        
        create table transaction(
            type varchar not null,
            time timestamp not null,
            source integer
                constraint transaction_source_foreign_key
                    references account(number),
            destination integer
                constraint transaction_destination_foreign_key
                    references account(number),
            amount real not null,
            id serial not null
                constraint transaction_primary_key
                    primary key
        );
        
        create table latest_balance(
            number integer not null
                constraint latest_balance_primary_key
                    primary key
                constraint latest_balance_account_foreign_key
                    references account(number),
            amount real not null
        );
        
        create table snapshot_log(
            id serial not null
                constraint snapshot_log_primary_key
                    primary key,
            time timestamp not null
        );   
    ''')


if __name__ == '__main__':
    main()
