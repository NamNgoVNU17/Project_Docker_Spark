import sqlite3
import random
import string

# Kết nối tới cơ sở dữ liệu SQLite (hoặc tạo mới nếu chưa tồn tại)
conn = sqlite3.connect('example.db')
cursor = conn.cursor()

# Tạo bảng
cursor.execute('''
CREATE TABLE IF NOT EXISTS my_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    age INTEGER,
    address TEXT
)
''')

# Hàm tạo dữ liệu ngẫu nhiên
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def random_age():
    return random.randint(18, 90)

def random_address():
    return random_string(10) + ' street'

# Chèn 1,000,000 hàng
for i in range(1000000):
    name = random_string(8)
    age = random_age()
    address = random_address()
    cursor.execute('''
    INSERT INTO my_table (name, age, address) VALUES (?, ?, ?)
    ''', (name, age, address))

    if (i + 1) % 10000 == 0:
        print(f'{i + 1} rows inserted...')
        conn.commit()  # Lưu thay đổi mỗi 10,000 hàng để giảm thiểu sử dụng bộ nhớ

# Lưu thay đổi và đóng kết nối
conn.commit()
conn.close()

print('Done inserting 1,000,000 rows.')
