from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import time
# Đường dẫn đến driver JDBC SQLite
jdbc_driver_path = "/sqlite_jdbc/sqlite-jdbc-3.46.0.0.jar"

# Khởi tạo phiên Spark với đường dẫn đến driver JDBC
spark = SparkSession.builder.appName("Ví dụ Spark SQLite").config("spark.driver.extraClassPath", jdbc_driver_path) \
    .getOrCreate()
       


# Tải cơ sở dữ liệu SQLite vào DataFrame của Spark
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .load()

# Hiển thị schema của DataFrame
df.printSchema()


# Thao tác Đọc - Hiển thị 5 hàng đầu tiên
print("Thao tác Đọc - 5 hàng đầu tiên:")
df.show(5)




# Thao tác Tạo - Chèn dữ liệu mới
print("Thao tác Tạo - Chèn thêm vào hàng cuối hàng 1000001")
du_lieu_moi = [(1000001, 'NGO HOAI NAM', 22, 'THAI BINH')]
df_moi = spark.createDataFrame(du_lieu_moi, ["id", "name", "age","address"])
df_moi.write.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .mode("append") \
    .save()
# Thao tác Đọc - Xác nhận việc chèn
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .load()
df.filter(df.id == 1000001).show()




print("Thao tác Cập nhật - Đã cập nhật tuổi của NGO HOAI NAM 22 thành 32")
# Thao tác Cập nhật - Đọc, Sửa đổi và Ghi lại
df = df.withColumn("age", when(df.id == 1000001, 32).otherwise(df.address))
df.filter(df.id == 1000001).show()
df.write.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .mode("overwrite") \
    .save()
# Thao tác Đọc - Xác nhận cập nhật
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .load()




print("Thao tác Xóa - Đã xóa NGO HOAI NAM")
# Thao tác Xóa - Đọc, Lọc và Ghi lại
df = df.filter(df.id != 1000001)
df.write.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .mode("overwrite") \
    .save()
# Thao tác Đọc - Xác nhận việc xóa
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:example.db") \
    .option("dbtable", "my_table") \
    .load()
df.filter(df.id == 1000001).show()





# Minh họa câu lệnh WHERE và tác động của nó đến hiệu suất


# Không có câu lệnh WHERE
start_time = time.time()
df.count()
print(f"Đếm không có câu lệnh WHERE: {time.time() - start_time} giây")

# Có câu lệnh WHERE
start_time = time.time()
df.filter(df.age > 50).count()
print(f"Đếm với câu lệnh WHERE (tuổi > 50): {time.time() - start_time} giây")

spark.stop()
