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
        create_procedures(cursor)

        menu(cursor)

        cursor.close()
        connection.close()

    except (Exception, psycopg2.Error) as exception:
        print("Error:", exception)


def menu(cursor):
    print("* BanQ - version 1.0 *")

    while True:
        print("[1] Register")
        print("[2] Login")
        print("[3] Deposit")
        print("[4] Withdraw")
        print("[5] Transfer")
        print("[6] Interest Payment")
        print("[7] Update Balances")
        print("[8] Check Balance")
        print("[9] Exit")

        choice = int(input())

        if choice == 1:
            register(cursor)

        elif choice == 2:
            login(cursor)

        elif choice == 3:
            deposit(cursor)

        elif choice == 4:
            withdraw(cursor)

        elif choice == 5:
            transfer(cursor)

        elif choice == 6:
            interest_payment(cursor)

        elif choice == 7:
            update_balances(cursor)

        elif choice == 8:
            check_balance(cursor)

        elif choice == 9:
            exit(0)

        else:
            print("Error: invalid input")


def register(cursor):
    try:
        username = input("Username: ")
        password = input("Password: ")
        first_name = input("First Name: ")
        last_name = input("Last Name: ")
        national_id = input("National Id: ")
        date_of_birth = input("Date of birth (YYYY-MM-DD):")
        interest_rate = input("Interest Rate: ")
        type_of_account = input("Client or Employee?")
        cursor.execute(f'''
            call register(
                '{username}'::varchar,
                '{password}'::varchar,
                '{first_name}'::varchar,
                '{last_name}'::varchar,
                {int(national_id)}::int8,
                to_date('{date_of_birth}', 'YYYY-MM-DD'),
                {float(interest_rate)}::float4,
                '{type_of_account.lower()}'::varchar
            )
        ''')
        print("Registered successfully")

    except Exception as exception:
        print("Error while registration:", exception)


def login(cursor):
    try:
        username = input("Username: ")
        password = input("Password: ")
        cursor.execute(f'''
            call login(
                '{username}'::varchar,
                '{password}'::varchar
            )
        ''')
        print("Logged in successfully")

    except Exception as exception:
        print("Error while logging in:", exception)


def deposit(cursor):
    try:
        amount = input("Amount: ")
        cursor.execute(f'''
            call deposit(
                {float(amount)}::float4
            )
        ''')
        print("The deposit has been successfully made")

    except Exception as exception:
        print("Error while making the deposit:", exception)


def withdraw(cursor):
    try:
        amount = input("Amount: ")
        cursor.execute(f'''
            call withdraw(
                {float(amount)}::float4
            )
        ''')
        print("The withdrawal has been successfully made")

    except Exception as exception:
        print("Error while making the withdrawal:", exception)


def transfer(cursor):
    try:
        amount = input("Amount: ")
        destination = input("Destination: ")
        cursor.execute(f'''
            call transfer(
                {float(amount)}::float4,
                {int(destination)}::int8
            )
        ''')
        print("The transfer has been successfully done")

    except Exception as exception:
        print("Error while doing the transfer:", exception)


def interest_payment(cursor):
    try:
        cursor.execute(f'''
            call interest_payment()
        ''')
        print("The interest has been successfully paid")

    except Exception as exception:
        print("Error while paying interest:", exception)


def update_balances(cursor):
    try:
        cursor.execute(f'''
            call update_balances()
        ''')
        print("The update has been successfully done")

    except Exception as exception:
        print("Error while updating balances:", exception)


def check_balance(cursor):
    try:
        cursor.execute(f'''
            select * from check_balance()
        ''')
        print("Balance:", int(cursor.fetchall()[0][0]))

    except Exception as exception:
        print("Error while checking balances:", exception)


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


def create_procedures(cursor):
    create_register_procedure(cursor)
    create_login_procedure(cursor)

    create_deposit_procedure(cursor)
    create_withdraw_procedure(cursor)
    create_transfer_procedure(cursor)
    create_interest_payment_procedure(cursor)

    create_update_balances_procedure(cursor)
    create_check_balance_function(cursor)


def create_register_procedure(cursor):
    cursor.execute('''
        create or replace procedure register(
            username_p varchar,
            password_p varchar,
            first_name_p varchar,
            last_name_p varchar,
            national_id_p int8,
            date_of_birth_p date,
            interest_rate_p float4,
            type_p varchar
        )
        language plpgsql
        as $$
        declare
            interest_rate_v float4;
            years_of_age integer;
        begin
            interest_rate_v := interest_rate_p;
            if type_p = 'employee' then
                interest_rate_v := 0;
            end if;

            select extract(year from age(current_date, date_of_birth_p)) into years_of_age;

            if years_of_age < 13 then
                raise exception 'age is less than 13';
            end if;

            insert into account(number, username, password, first_name, last_name,
                                national_id, date_of_birth,interest_rate, type)
                values (1, username_p, hashtext(password_p), first_name_p, last_name_p,
                        national_id_p, date_of_birth_p, interest_rate_v, type_p);
        end $$;

        create or replace function update_user_number()
            returns trigger as $$
        declare
            user_number integer;
            unique_username varchar(100);
        begin
            unique_username := '';
            while unique_username = ''
                loop
                    unique_username := to_char(floor(random() * 10000000000000000), 'fm0000000000000000');
                    select into unique_username case
                        when exists(select 1 from account where username = unique_username) then ''
                        else unique_username end;
                end loop;
            user_number := (abs(hashtext(new.first_name || new.last_name || unique_username))) % 10000000000000000;
            update account set number = user_number where account.username = new.username;
            insert into latest_balance (number, amount) values (user_number, 0);
            return new;
        end;
        $$ language plpgsql;

        create trigger calculate_account_number
            after insert on account for each row
        execute function update_user_number();
    ''')


def create_login_procedure(cursor):
    cursor.execute('''
        create or replace procedure login(
            username_p varchar,
            password_p varchar
        )
        language plpgsql
        as $$
        declare
            hash_of_password varchar;
            select_username varchar;
        begin
            hash_of_password := hashtext(password_p);
            select username
                from account
                where account.username = username_p and account.password = hash_of_password
                into select_username;
            
            if select_username is null then
                raise exception 'invalid input';
            end if;
            
            insert into login_log (username, time) values (username_p, clock_timestamp());
        end
        $$;
    ''')


def create_deposit_procedure(cursor):
    cursor.execute('''
        create or replace procedure deposit(
            amount_p float4
        )
        language plpgsql
        as $$
        declare
            source_number int8;
        begin
            select number
            from account
            where account.username = (
                select username
                from login_log login_log1
                where login_log1.time = (
                    select max(login_log2.time)
                    from login_log login_log2
                )
            )
            into source_number;
        
            insert into transaction (type, time, source, destination, amount)
                values ('deposit', clock_timestamp(), source_number, null, amount_p);
        end
        $$; 
    ''')


def create_withdraw_procedure(cursor):
    cursor.execute('''
        create or replace procedure withdraw(
            amount_p float4
        )
        language plpgsql
        as $$
        declare
            destination_number int8;
        begin
            select number
            from account
            where account.username = (
                select username
                from login_log login_log1
                where login_log1.time = (
                    select max(login_log2.time)
                    from login_log login_log2
                )
            )
            into destination_number;
        
            insert into transaction (type, time, source, destination, amount)
                values ('withdraw', clock_timestamp(), null, destination_number, amount_p);
        end
        $$;
    ''')


def create_transfer_procedure(cursor):
    cursor.execute('''
        create or replace procedure transfer(
            amount_p float4,
            destination_p int8
        )
        language plpgsql
        as $$
        declare
            source_number int8;
        begin
            select number
            from account
            where account.username = (
                select username
                from login_log login_log1
                where login_log1.time = (
                    select max(login_log2.time)
                    from login_log login_log2
                )
            )
            into source_number;
        
            insert into transaction (type, time, source, destination, amount)
                values ('transfer', clock_timestamp(), source_number, destination_p, amount_p);
        end
        $$;
    ''')


def create_interest_payment_procedure(cursor):
    cursor.execute('''
        create or replace procedure interest_payment()
        language plpgsql
        as $$
        declare
            balance float4;
            updated_balance float4;
            interest float4;
            source_number int8;
        begin
            for source_number in
                select number from account
                where account.interest_rate is not null and account.interest_rate <> 0
                loop
                    select amount from latest_balance where number = source_number into balance;
                    select interest_rate from account where number = source_number into interest;
                    updated_balance = (balance * interest) / 100;
                    insert into transaction(type, time, source, destination, amount)
                    values ('interest', clock_timestamp(), source_number, null, updated_balance);
                end loop;
        end
        $$;
    ''')


def create_update_balances_procedure(cursor):
    cursor.execute('''
        create or replace procedure update_balances()
        language plpgsql
        as $$
        declare
            last_snapshot_time timestamp;
            transaction_id int4;
            transaction_type varchar;
            source_number int4;
            destination_number int4;
            amount_v float4;
            source_last_amount float4;
            destination_last_amount float4;
            destination_type varchar;
        begin
            select max(time) from snapshot_log into last_snapshot_time;
        
            for transaction_id in select id from transaction
                                    where transaction.time >= last_snapshot_time or last_snapshot_time is null
                loop
                    select type into transaction_type from transaction where id = transaction_id;
                    select source into source_number from transaction where id = transaction_id;
                    select destination into destination_number from transaction where id = transaction_id;
                    select amount into amount_v from transaction where id = transaction_id;
                    select type into destination_type from account where number = transaction_id;
        
                    if transaction_type = 'deposit' then
                        select amount into source_last_amount from latest_balance where number = source_number;
                        update latest_balance set amount = source_last_amount + amount_v where number = source_number;
                    end if;
        
                    if transaction_type = 'withdraw' then
                        select amount into destination_last_amount from latest_balance where number = destination_number;
                        if destination_last_amount - amount_v < 0 and destination_type = 'employee' then
                            update latest_balance set amount = destination_last_amount - amount_v where number = destination_number;
                        end if;
                        if destination_last_amount - amount_v >= 0 then
                            update latest_balance set amount = destination_last_amount - amount_v where number = destination_number;
                        end if;
                    end if;
        
                    if transaction_type = 'transfer' then
                        select amount into source_last_amount from latest_balance where number = source_number;
                        select amount into destination_last_amount from latest_balance where number = destination_number;
        
                        select amount into destination_last_amount from latest_balance where number = destination_number;
                        if source_last_amount - amount_v < 0 and destination_type = 'employee' then
                            update latest_balance set amount = source_last_amount - amount_v where number = source_number;
                            update latest_balance set amount = destination_last_amount + amount_v where number = destination_number;
                        end if;
                        if source_last_amount - amount_v >= 0 then
                            update latest_balance set amount = source_last_amount - amount_v where number = source_number;
                            update latest_balance set amount = destination_last_amount + amount_v where number = destination_number;
                        end if;
                    end if;
        
                    if transaction_type = 'interest' then
                        select amount into source_last_amount from latest_balance where number = source_number;
                        update latest_balance set amount = source_last_amount + amount_v where number = source_number;
                    end if;
                end loop;
                
            insert into snapshot_log (time) values (clock_timestamp());
        end
        $$;
    ''')


def create_check_balance_function(cursor):
    cursor.execute('''
        create or replace function check_balance()
            returns decimal(10, 2)
        language plpgsql
        as $$
        declare
            account_number int8;
            balance float4;
        begin
            select number
            from account
            where account.username = (
                select username
                from login_log login_log1
                where login_log1.time = (
                    select max(login_log2.time)
                    from login_log login_log2
                )
            )
            into account_number;
            select amount into balance from latest_balance where number = account_number;
            return balance;
        end
        $$;
    ''')


if __name__ == '__main__':
    main()
