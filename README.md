# Lab_01
# 1. Cài đặt PySpark (Chạy trên Google Colab)
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!pip install -q pyspark

# 2. Khởi tạo Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random

spark = SparkSession.builder.appName("Lab_PySpark_Functions").getOrCreate()


# 3. Tạo dữ liệu giả lập (DataFrame)
data = []
cities = ["Hanoi", "HCM", "Danang", "Cantho"]
products = ["Laptop", "Mouse", "Keyboard", "Headphone", "Monitor"]
categories = ["Computer", "Accessory", "Accessory", "Audio", "Display"]

for i in range(1, 101): # Tạo 100 dòng
    prod_idx = random.randint(0, 4)
    row = (
        f"TRX_{i:03d}",                 # TransactionID
        products[prod_idx],             # Product
        categories[prod_idx],           # Category
        random.randint(10, 50) * 10,    # Price ($100 - $500)
        random.randint(1, 5),           # Quantity
        random.choice(cities)           # City
    )
    data.append(row)

columns = ["TransactionID", "Product", "Category", "Price", "Quantity", "City"]
df = spark.createDataFrame(data, columns)

print("✅ Đã tạo xong dữ liệu mẫu!")

E: Failed to fetch http://security.ubuntu.com/ubuntu/pool/universe/o/openjdk-8/openjdk-8-jre-headless_8u472-ga-1%7e22.04_amd64.deb  404  Not Found [IP: 91.189.91.82 80]
E: Failed to fetch http://security.ubuntu.com/ubuntu/pool/universe/o/openjdk-8/openjdk-8-jdk-headless_8u472-ga-1%7e22.04_amd64.deb  404  Not Found [IP: 91.189.91.82 80]
E: Unable to fetch some archives, maybe run apt-get update or try with --fix-missing?
✅ Đã tạo xong dữ liệu mẫu!

# Xem cấu trúc dữ liệu (Tên cột, kiểu dữ liệu)
print("--- Schema ---")
df.printSchema()

# Xem 5 dòng đầu tiên
print("--- 5 Dòng đầu ---")
df.show(5)

# Thống kê mô tả (Count, Mean, Min, Max) cột Giá
print("--- Thống kê giá ---")
df.describe("Price").show()

--- Schema ---
root
 |-- TransactionID: string (nullable = true)
 |-- Product: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Price: long (nullable = true)
 |-- Quantity: long (nullable = true)
 |-- City: string (nullable = true)

--- 5 Dòng đầu ---
+-------------+---------+---------+-----+--------+------+
|TransactionID|  Product| Category|Price|Quantity|  City|
+-------------+---------+---------+-----+--------+------+
|      TRX_001|   Laptop| Computer|  300|       5|Cantho|
|      TRX_002| Keyboard|Accessory|  150|       4|   HCM|
|      TRX_003| Keyboard|Accessory|  350|       2| Hanoi|
|      TRX_004|Headphone|    Audio|  460|       2|   HCM|
|      TRX_005|Headphone|    Audio|  460|       2|   HCM|
+-------------+---------+---------+-----+--------+------+
only showing top 5 rows
--- Thống kê giá ---
+-------+------------------+
|summary|             Price|
+-------+------------------+
|  count|               100|
|   mean|             306.4|
| stddev|121.26871406138108|
|    min|               100|
|    max|               500|
+-------+------------------+

# Lọc theo 2 điều kiện kết hợp
# Lưu ý: Mỗi điều kiện phải để trong ngoặc đơn ()
high_value_hanoi = df.filter((col("City") == "Hanoi") & (col("Price") > 300))

high_value_hanoi.show()

+-------------+---------+---------+-----+--------+-----+
|TransactionID|  Product| Category|Price|Quantity| City|
+-------------+---------+---------+-----+--------+-----+
|      TRX_003| Keyboard|Accessory|  350|       2|Hanoi|
|      TRX_007|   Laptop| Computer|  360|       4|Hanoi|
|      TRX_028|   Laptop| Computer|  360|       4|Hanoi|
|      TRX_031|Headphone|    Audio|  320|       1|Hanoi|
|      TRX_036| Keyboard|Accessory|  380|       4|Hanoi|
|      TRX_057|    Mouse|Accessory|  480|       3|Hanoi|
|      TRX_061|Headphone|    Audio|  400|       5|Hanoi|
|      TRX_062|   Laptop| Computer|  400|       3|Hanoi|
|      TRX_079|    Mouse|Accessory|  440|       3|Hanoi|
|      TRX_088|    Mouse|Accessory|  450|       3|Hanoi|
|      TRX_094|  Monitor|  Display|  440|       3|Hanoi|
+-------------+---------+---------+-----+--------+-----+

# Tạo cột mới tên là "TotalValue"
df_processed = df.withColumn("TotalValue", col("Price") * col("Quantity"))

df_processed.show(5)

+-------------+---------+---------+-----+--------+------+----------+
|TransactionID|  Product| Category|Price|Quantity|  City|TotalValue|
+-------------+---------+---------+-----+--------+------+----------+
|      TRX_001|   Laptop| Computer|  300|       5|Cantho|      1500|
|      TRX_002| Keyboard|Accessory|  150|       4|   HCM|       600|
|      TRX_003| Keyboard|Accessory|  350|       2| Hanoi|       700|
|      TRX_004|Headphone|    Audio|  460|       2|   HCM|       920|
|      TRX_005|Headphone|    Audio|  460|       2|   HCM|       920|
+-------------+---------+---------+-----+--------+------+----------+
only showing top 5 rows

# Bước 1: Phải có cột TotalValue trước (lấy từ phần trước)
df_cal = df.withColumn("TotalValue", col("Price") * col("Quantity"))

# Bước 2: GroupBy và Sum
report_city = df_cal.groupBy("City") \
                  .agg(sum("TotalValue").alias("Revenue")) \
                  .orderBy(col("Revenue").desc()) # Sắp xếp giảm dần

report_city.show()

+------+-------+
|  City|Revenue|
+------+-------+
|Danang|  27140|
| Hanoi|  24680|
|   HCM|  20630|
|Cantho|  18730|
+------+-------+

# Bước 1: Tạo một "View" tạm thời (giống như bảng ảo)
df.createOrReplaceTempView("SalesTable")

# Bước 2: Viết SQL
sql_result = spark.sql("SELECT Product, Price, City FROM SalesTable WHERE Price > 400")

sql_result.show()

+---------+-----+------+
|  Product|Price|  City|
+---------+-----+------+
|Headphone|  460|   HCM|
|Headphone|  460|   HCM|
| Keyboard|  450|Danang|
|   Laptop|  470|Danang|
|   Laptop|  420|Danang|
|   Laptop|  490|   HCM|
|Headphone|  450|Cantho|
|  Monitor|  440|   HCM|
|Headphone|  490|Danang|
|    Mouse|  460|   HCM|
|Headphone|  480|Danang|
|    Mouse|  460|Cantho|
|    Mouse|  420|   HCM|
|  Monitor|  440|   HCM|
| Keyboard|  460|Danang|
| Keyboard|  440|Danang|
|    Mouse|  480| Hanoi|
| Keyboard|  480|Danang|
|    Mouse|  420|Danang|
|    Mouse|  440|Danang|
+---------+-----+------+
only showing top 20 rows

# Lưu kết quả ra file Parquet (định dạng tối ưu cho Big Data)
output_path = "processed_sales_data"

# mode("overwrite"): Ghi đè nếu folder đã tồn tại
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Đã lưu dữ liệu vào thư mục: {output_path}")

# Kiểm tra lại bằng cách đọc lên
spark.read.parquet(output_path).show(3)

✅ Đã lưu dữ liệu vào thư mục: processed_sales_data
+-------------+--------+---------+-----+--------+------+
|TransactionID| Product| Category|Price|Quantity|  City|
+-------------+--------+---------+-----+--------+------+
|      TRX_051|   Mouse|Accessory|  190|       4|   HCM|
|      TRX_052|Keyboard|Accessory|  440|       4|Danang|
|      TRX_053|  Laptop| Computer|  110|       2|Cantho|
+-------------+--------+---------+-----+--------+------+
only showing top 3 rows
