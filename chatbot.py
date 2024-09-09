import ollama
from sqlalchemy import create_engine
from pymongo import MongoClient
from sqlalchemy.exc import SQLAlchemyError
from pymongo.errors import PyMongoError
import logging
import requests
import json
import os
from dotenv import load_dotenv

logging.basicConfig(filename='chatbot.log', level=logging.INFO)

load_dotenv()  # Load environment variables from .env file

class DatabaseConnector:
    def __init__(self):
        self.connections = {}

    def connect_sqlite(self, db_path):
        engine = create_engine(f'sqlite:///{db_path}')
        self.connections['sqlite'] = engine

    def connect_mysql(self, host, user, password, database):
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        self.connections['mysql'] = engine

    def connect_mariadb(self, host, user, password, database):
        engine = create_engine(f'mariadb+pymysql://{user}:{password}@{host}/{database}')
        self.connections['mariadb'] = engine

    def connect_postgresql(self, host, user, password, database):
        engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')
        self.connections['postgresql'] = engine

    def connect_mongodb(self, host, port, database):
        client = MongoClient(host, port)
        self.connections['mongodb'] = client[database]

class Chatbot:
    def __init__(self, model_name=None):
        self.model_name = model_name or os.getenv('CHATBOT_MODEL', 'llama3.1')
        self.db_connector = DatabaseConnector()
        self.context = []
        self.api_base = os.getenv('OLLAMA_API_BASE', 'http://localhost:11434/api')

    def connect_to_database(self, db_type, **kwargs):
        if db_type == 'sqlite':
            self.db_connector.connect_sqlite(kwargs['db_path'])
        elif db_type in ['mysql', 'mariadb']:
            self.db_connector.connect_mysql(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'])
        elif db_type == 'postgresql':
            self.db_connector.connect_postgresql(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'])
        elif db_type == 'mongodb':
            self.db_connector.connect_mongodb(kwargs['host'], kwargs['port'], kwargs['database'])

    def execute_query(self, db_type, query):
        try:
            if db_type in ['sqlite', 'mysql', 'mariadb', 'postgresql']:
                connection = self.db_connector.connections[db_type]
                with connection.connect() as conn:
                    result = conn.execute(query)
                    return result.fetchall()
            elif db_type == 'mongodb':
                db = self.db_connector.connections[db_type]
                return list(db.command(query))
        except (SQLAlchemyError, PyMongoError) as e:
            print(f"Database error: {str(e)}")
            return None

    def generate_response(self, prompt):
        url = f"{self.api_base}/chat"
        
        payload = {
            "model": self.model_name,
            "messages": self.context + [{"role": "user", "content": prompt}],
            "stream": False
        }
        
        response = requests.post(url, json=payload)
        response_json = response.json()
        
        if response.status_code == 200:
            assistant_message = response_json['message']['content']
            self.context.append({"role": "user", "content": prompt})
            self.context.append({"role": "assistant", "content": assistant_message})
            return assistant_message
        else:
            return f"Error: {response.status_code}, {response_json.get('error', 'Unknown error')}"

    def pull_model(self):
        url = f"{self.api_base}/pull"
        payload = {"name": self.model_name}
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print(f"Model {self.model_name} pulled successfully.")
        else:
            print(f"Error pulling model: {response.status_code}")

    def chat(self):
        print(f"Chatbot: Hello! I'm your database-connected chatbot. How can I help you today?")
        while True:
            user_input = input("You: ")
            if user_input.lower() in ['exit', 'quit', 'bye']:
                print("Chatbot: Goodbye!")
                break
            response = self.generate_response(user_input)
            print(f"Chatbot: {response}")

if __name__ == "__main__":
    chatbot = Chatbot()
    chatbot.pull_model()
    
    # Connect to databases using environment variables
    chatbot.connect_to_database('sqlite', db_path=os.getenv('SQLITE_DB_PATH', 'example.db'))
    chatbot.connect_to_database('mysql', 
                                host=os.getenv('MYSQL_HOST', 'localhost'),
                                user=os.getenv('MYSQL_USER', 'root'),
                                password=os.getenv('MYSQL_PASSWORD', ''),
                                database=os.getenv('MYSQL_DATABASE', 'mydb'))
    chatbot.connect_to_database('postgresql', 
                                host=os.getenv('POSTGRES_HOST', 'localhost'),
                                user=os.getenv('POSTGRES_USER', 'postgres'),
                                password=os.getenv('POSTGRES_PASSWORD', ''),
                                database=os.getenv('POSTGRES_DATABASE', 'mydb'))
    chatbot.connect_to_database('mongodb', 
                                host=os.getenv('MONGO_HOST', 'localhost'),
                                port=int(os.getenv('MONGO_PORT', 27017)),
                                database=os.getenv('MONGO_DATABASE', 'mydb'))
    
    chatbot.chat()