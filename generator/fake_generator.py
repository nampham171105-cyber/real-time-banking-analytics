import psycopg2
from faker import Faker
from decimal import Decimal, ROUND_DOWN
import time
import random
import os
from dotenv import load_dotenv

load_dotenv()

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

ACCOUNT_TYPES = [
    "SAVING",
    "CHECKING",
    "PREMIUM"
]

fake = Faker()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cur = conn.cursor()

customers = []
accounts = []

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val),float(max_val)))) # random trong khoảng với kiểu dữ liệu decimal(chính xác nhât)
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN) # làm tròn bằng cách cắt đi phần sau

# ----------CUSTOMER----------

def create_customer():
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.unique.email()

    cur.execute(
        """
        INSERT INTO customer 
        (first_name, last_name, email) 
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (first_name, last_name, email)
    )
    customer_id = cur.fetchone()[0]
    customers.append({
        "id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email
    })

def update_customer_info():
    customer = random.choice(customers)
    update = random.choice(["email","first_name","last_name"])
    if update == "email":
        email = fake.unique.email()
        cur.execute(
            """
            UPDATE customer
            SET email = %s
            WHERE id = %s
            """,
            (email, customer["id"])
        )
        customer["email"] = email
    elif update == "first_name":
        first_name = fake.first_name()
        cur.execute(
            """
            UPDATE customer
            SET first_name = %s
            WHERE id = %s
            """,
            (first_name, customer["id"])
        )
        customer["first_name"] = first_name
    else:
        last_name = fake.last_name()
        cur.execute(
            """
            UPDATE customer
            SET last_name = %s
            WHERE id = %s
            """,
            (last_name, customer["id"])
        )
        customer["last_name"] = last_name
    

def delete_customer():
    customer = random.choice(customers)
    cur.execute (
        """
        DELETE 
        FROM customer
        WHERE id = %s
        """,
        (customer["id"],)
    )
    customers.remove(customer)

# ---------ACCOUNT------------

def open_account():
    customer = random.choice(customers)
    account_type = random.choice(ACCOUNT_TYPES)
    balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
    cur.execute(
        """
        INSERT INTO account 
        (customer_id, account_type, balance, currency, status)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (customer["id"], account_type, balance, "USD", "ACTIVE")
    )
    account_id = cur.fetchone()[0]
    accounts.append({
        "id": account_id,
        "customer_id": customer["id"],
        "balance": balance,
        "status": "ACTIVE",
        "account_type": account_type
    })

def freeze_account():
    account = random.choice(accounts)

    account["status"] = "FROZEN"

    cur.execute(
        """
        UPDATE account
        SET status=%s
        WHERE id=%s
        """,
        ("FROZEN", account["id"])
    )

def unfreeze_account():
    account = random.choice(accounts)

    account["status"] = "ACTIVE"

    cur.execute(
        """
        UPDATE account
        SET status=%s
        WHERE id=%s
        """,
        ("ACTIVE", account["id"])
    )

def change_account_type():
    account = random.choice(accounts)
    current = account["account_type"]
    candidates = [ t for t in ACCOUNT_TYPES if t != current ]
    new_type = random.choice(candidates)
    cur.execute(
        """
        UPDATE account
        SET account_type = %s
        WHERE id = %s
        """,
        (new_type, account["id"])
    )
    account["account_type"] = new_type

#---------TRACSACTION---------

def deposit():
    account = random.choice(accounts)
    amount = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)

    if account["status"] != 'ACTIVE':
        cur.execute(
            """
            INSERT INTO transaction (account_id, txn_type, amount, txn_status, error_code)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (account["id"], "DEPOSIT", amount, "FAILED", "ACCOUNT_INACTIVE")
        )
        return

    cur.execute(
        """UPDATE account
           SET balance = balance + %s
           WHERE id = %s       
        """,
        (amount, account["id"])
    )
    cur.execute(
        """
        INSERT INTO transaction (account_id, txn_type, amount, txn_status)
        VALUES (%s, %s, %s, %s)
        """,
        (account["id"], "DEPOSIT", amount, "COMPLETED")
    )
    account["balance"] += amount

def withdraw():
    account = random.choice(accounts)
    amount = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)

    if account["status"] != 'ACTIVE':
        cur.execute(
            """
            INSERT INTO transaction (account_id, txn_type, amount, txn_status, error_code)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (account["id"], "WITHDRAW", amount, "FAILED", "ACCOUNT_INACTIVE")
        )
        return
    
    if account["balance"] < amount:
        cur.execute(
            """
            INSERT INTO transaction (account_id, txn_type, amount, txn_status, error_code)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (account["id"], "WITHDRAW", amount, "FAILED", "INSUFFICIENT_BALANCE")
        )
        return
 
    cur.execute(
        """
        UPDATE account
        SET balance = balance - %s
        WHERE id = %s
        """,
        (amount, account["id"])
    )
    cur.execute(
        """
        INSERT INTO transaction
        (account_id, txn_type, amount, txn_status)
        VALUES (%s, %s, %s, %s)
        """,
        (account["id"],"WITHDRAW", amount, "COMPLETED")
    )
    account["balance"] -= amount

def transfer():
    sender = random.choice(accounts)
    receiver = random.choice([a for a in accounts if a["id"] != sender["id"]])

    amount = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)

    failure_reason = None
    if sender["status"] != 'ACTIVE':
        failure_reason = "SENDER_INACTIVE"
    elif receiver["status"] != 'ACTIVE':
        failure_reason = "RECEIVER_INACTIVE"
    elif sender["balance"] < amount:
        failure_reason = "INSUFFICIENT_BALANCE"

    if failure_reason:
        cur.execute(
            """
            INSERT INTO transaction
            (account_id, related_account_id, txn_type, amount, txn_status, error_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (sender["id"], receiver["id"], "TRANSFER", amount, "FAILED", failure_reason)
        )
        return
    # conn.autocommit = False
    try:
        cur.execute(
            """
            UPDATE account
            SET balance = balance - %s
            WHERE id = %s
            """,
            (amount, sender["id"])
        )

        cur.execute(
            """
            UPDATE account
            SET balance = balance + %s
            WHERE id = %s
            """,
            (amount, receiver["id"])
        )

        cur.execute(
            """
            INSERT INTO transaction
            (account_id, related_account_id, txn_type, amount, txn_status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (sender["id"], receiver["id"], "TRANSFER", amount, "COMPLETED")
        )
        #conn.commit()
        sender["balance"] -= amount
        receiver["balance"] += amount
    except Exception as e:
        #conn.rollback()
        print(f"Transfer failed {e}")

def generate_initial_customers(n):
    """Hàm tạo n khách hàng và tài khoản ban đầu"""
    print(f"\n Đang tạo {n} khách hàng và tài khoản. Vui lòng đợi...")
    
    for _ in range(n):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        
        cur.execute(
            """
            INSERT INTO customer 
            (first_name, last_name, email) 
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (first_name, last_name, email)
        )
        customer_id = cur.fetchone()[0]
        customers.append({
            "id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email
        })

        # 2. Sinh dữ liệu tài khoản (Account) cho khách hàng đó
        account_type = random.choice(ACCOUNT_TYPES)
        initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
        
        cur.execute(
            """
            INSERT INTO account 
            (customer_id, account_type, balance, currency, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (customer_id, account_type, initial_balance, "USD", "ACTIVE")
        )
        account_id = cur.fetchone()[0]
        accounts.append({
            "id": account_id,
            "customer_id": customer_id,
            "balance": initial_balance,
            "status": "ACTIVE",
            "account_type": account_type
        })
    conn.commit()        
    print(f"Đã khởi tạo thành công {n} khách hàng và tài khoản vào hệ thống!\n")

# ----------MAIN LOOP------------

TPS = 1500

try:
    n_str = input("Nhập số lượng khách hàng ban đầu cần tạo: ")
    n = int(n_str)

    generate_initial_customers(n)

    while True:
        start = time.perf_counter()

        events = random.choices(
                    population = [
                        deposit,
                        withdraw,
                        transfer,
                        update_customer_info,
                        open_account,
                        freeze_account,
                        unfreeze_account,
                        change_account_type,
                        #delete_customer
                    ],
                    weights = [
                        25,
                        25,
                        30,
                        5,
                        5,
                        4,
                        3,
                        3
                    ],
                    k = TPS
                )
        for event in events:
            event()
        conn.commit()

        elapsed = time.perf_counter() - start

        if elapsed < 1:
            time.sleep(1 - elapsed)

        print(f"Executed {TPS} events in {elapsed:.3f}s")
        
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()



