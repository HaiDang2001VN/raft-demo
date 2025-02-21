import sqlite3

class PBFTDatabase:
    def __init__(self, db_name="pbft.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS blockchain (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_hash TEXT UNIQUE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        self.conn.commit()

    def add_block(self, block_hash):
        self.cursor.execute("INSERT OR IGNORE INTO blockchain (block_hash) VALUES (?)", (block_hash,))
        self.conn.commit()

    def get_blocks(self):
        self.cursor.execute("SELECT * FROM blockchain")
        return self.cursor.fetchall()