
import tkinter as tk
from tkinter import ttk
from pyspark.sql import SparkSession
from pyspark.sql.functions import year


# Hàm callback cho nút "Employees Selected"
def show_selected_data():
    # Đọc dữ liệu từ table "employees_selected"
    df_selected = spark.read.jdbc(url=jdbc_url, table=table_name_selected, properties=properties)

    # Xóa dữ liệu cũ trong table
    clear_table()

    # Thêm dữ liệu mới vào table
    add_data_to_table(df_selected)


# Hàm callback cho nút "Employees Filtered"
def show_filtered_data():
    # Đọc dữ liệu từ table "employees_filtered"
    df_filtered = spark.read.jdbc(url=jdbc_url, table=table_name_filtered, properties=properties)

    # Xóa dữ liệu cũ trong table
    clear_table()

    # Thêm dữ liệu mới vào table
    add_data_to_table(df_filtered)

# Hàm callback cho nút "Employees With High Salary"
def show_high_salary_data():
    # Đọc và lọc dữ liệu từ table "employees" với điều kiện salary > 5000
    query = "(SELECT * FROM employees WHERE salary > 5000) AS high_salary_employees"
    df_high_salary = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    # Xóa dữ liệu cũ trong table
    clear_table()

    # Thêm dữ liệu mới vào table
    add_data_to_table(df_high_salary)
    df_high_salary.write.jdbc(url=jdbc_url, table="high_salary_employees", mode="overwrite", properties=properties)


def show_recent_hires():
    # Đọc dữ liệu từ table "employees" và lọc dựa trên năm hire_date
    df = spark.read.jdbc(url=jdbc_url, table="employees", properties=properties)
    df_recent_hires = df.filter(year(df["hire_date"]) >= 2003)

    # Xóa dữ liệu cũ trong table
    clear_table()

    # Thêm dữ liệu mới vào table
    add_data_to_table(df_recent_hires)
    df_recent_hires.write.jdbc(url=jdbc_url, table="recent_hires", mode="overwrite", properties=properties)


# Hàm để xóa dữ liệu trong table
def clear_table():
    for row in table.get_children():
        table.delete(row)


# Hàm để thêm dữ liệu vào table
def add_data_to_table(df):
    for index, row in df.toPandas().iterrows():
        table.insert("", "end", values=list(row))


from pyspark.sql import SparkSession
# Tạo Spark session và thêm MySQL connector JAR vào classpath
spark = SparkSession.builder \
    .appName("MySQL to Spark") \
    .config("spark.driver.extraClassPath", "C:/mysql-connector-java-8.0.23.jar") \
    .config("spark.executor.extraClassPath", "C:/mysql-connector-java-8.0.23.jar") \
    .getOrCreate()

# Thông tin kết nối đến MySQL
jdbc_url = "jdbc:mysql://localhost:3306/mydatabase"
properties = {
    "user": "root",
    "password": "tandat2462003",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Đọc dữ liệu từ MySQL
df = spark.read.jdbc(url=jdbc_url, table="employees", properties=properties)
root = tk.Tk()
root.title("Employee Data")

# Tên bảng trong MySQL để lưu kết quả
table_name_selected = "employees_selected"
table_name_filtered = "employees_filtered"

# Tạo table
table = ttk.Treeview(root)

# Tạo thanh cuộn ngang cho table
scrollbar_x = ttk.Scrollbar(root, orient="horizontal", command=table.xview)
table.configure(xscrollcommand=scrollbar_x.set)
scrollbar_x.pack(side="bottom", fill="x")

# Tạo các cột trong table
table["columns"] = list(df.columns)

# Hiển thị tiêu đề cột
for col in df.columns:
    table.heading(col, text=col)

# Thêm dữ liệu vào table
for index, row in df.toPandas().iterrows():
    table.insert("", "end", values=list(row))

# Tạo Frame để chứa các nút
buttons_frame = tk.Frame(root)
buttons_frame.pack(side="top", fill="x", pady=10)

# Định nghĩa kích thước chung cho các nút
button_width = 20
button_height = 2

# Tạo nút "Employees Selected" và gán callback, sau đó đặt nó vào frame
btn_selected = tk.Button(buttons_frame, text="Employees Selected", command=show_selected_data, width=button_width, height=button_height)
btn_selected.pack(side="left", padx=5)

# Tạo nút "Employees Filtered" và gán callback, sau đó đặt nó vào frame
btn_filtered = tk.Button(buttons_frame, text="Employees Filtered", command=show_filtered_data, width=button_width, height=button_height)
btn_filtered.pack(side="left", padx=5)

# Tạo nút "Recent Hires" và gán callback, sau đó đặt nó vào frame
btn_recent_hires = tk.Button(buttons_frame, text="Recent Hires", command=show_recent_hires, width=button_width, height=button_height)
btn_recent_hires.pack(side="left", padx=5)

# Tạo nút "Employees With High Salary" và gán callback, sau đó đặt nó vào frame
btn_high_salary = tk.Button(buttons_frame, text="Employees With High Salary", command=show_high_salary_data, width=button_width, height=button_height)
btn_high_salary.pack(side="left", padx=5)

# Hiển thị table
table.pack(expand=True, fill="both")

# Hiển thị cửa sổ
root.mainloop()
