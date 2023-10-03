import mysql.connector
from dotenv import load_dotenv

from mysql.connector import Error

load_dotenv()


def brain(name, message):
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try:
            mydb = mysql.connector.connect(
                host="cloud.mindsdb.com",
                user="your_username",  # enter your mail
                password="your_password",  # enter your password
                port="3306"
            )
            break  # Connection successful, exit the loop
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            retries += 1
            if retries < max_retries:
                print("Retrying...")
                continue
            else:
                print("Max retries reached. Unable to connect.")
                exit()

    cursor = mydb.cursor(buffered=True)

    cursor.execute(f'''
    SELECT response from mindsdb.gpt_model
    WHERE
    author_username='{name}'
    AND text="{message}";
    ''')

    for x in cursor:
        return x

    cursor.close()
    mydb.close()


def handle_response(name, message):
    p_message = message.lower()
    reply = brain(name, p_message)

    return reply
