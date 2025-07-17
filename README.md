*Project: Xây dựng đường ống dữ liệu thu thập MetaData từ GooglePlay Store
Mục tiêu:
- Xây dựng luồng dữ liệu từ crawl -> transform -> load -> visualize
- Giám sát, lập lịch tác vụ bằng Airflow
  
1. Tổng quan hệ thống
  <img width="779" height="402" alt="image" src="https://github.com/user-attachments/assets/25a744e8-47bd-4283-beb6-d8322e9e7f2d" />
  
1.1 Thu thập dữ liệu
   <img width="1109" height="325" alt="image" src="https://github.com/user-attachments/assets/fa099df1-3325-4a7b-9e5f-fb5a9a53b381" />

   Dữ liệu được Crawl từ trang chủ của GooglePlay Store như sau:
   - Lấy Url gốc ban đầu là "https://play.google.com/store/games". Sau đó tiến hành thêm các tham số query vào Url để vào được các danh mục cụ thể
   - Trong các trang này ta tiến hành lấy tất cả đường link trỏ tới tất cả các game của lần crawl đó
   - Công nghệ sử dụng: Requests và Bs4
     
1.2 Lưu trữ dữ liệu thô
  Dữ liệu thô ở đây chính là các dataframe bao gồm 2 cột: "Url" và "content" - dạng html của url đó
  <img width="1180" height="368" alt="image" src="https://github.com/user-attachments/assets/95c371cf-6140-44b3-ba7d-af8c9219701b" />
- Dữ liệu thô được lưu ở định dạng parquet tại Hadoop:
  <img width="968" height="432" alt="image" src="https://github.com/user-attachments/assets/8abc2d5e-9fb1-406f-9709-ebab38712cdd" />
  
1.3 Xử lý dữ liệu
  - Dữ liệu được xử lý từng ngày theo lô bằng Spark: Sử dụng các hàm UDF để làm sạch
  Kiểm tra tốc độ xử lý của Spark Cluster ở chế độ Standalone như phía dưới:
<img width="1250" height="548" alt="image" src="https://github.com/user-attachments/assets/02fabf7b-04a8-4248-84f5-e2fb51171a04" />

1.4 Lập lịch tự động
<img width="1012" height="653" alt="image" src="https://github.com/user-attachments/assets/076883c8-67db-4424-873c-10fa68c8a243" />

2. Các kết quả đạt được
   - Top Game có lượt tải cao nhất
   <img width="617" height="287" alt="image" src="https://github.com/user-attachments/assets/42f4aa2a-a340-4e45-a1c1-0a725169116d" />
  
   - Phân bố Game theo độ tuổi
   <img width="700" height="272" alt="image" src="https://github.com/user-attachments/assets/dcc5f6d8-a1f0-498d-a3d9-59806a8f6649" />

   - Phân bố Game theo thể loại
   <img width="581" height="356" alt="image" src="https://github.com/user-attachments/assets/796095ec-f33d-4cb2-b547-c2fc6375713c" />


