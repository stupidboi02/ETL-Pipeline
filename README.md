# ETL Pipeline: Big Data Batch Processing

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.x-blue?logo=apache-airflow)
![Hadoop](https://img.shields.io/badge/Hadoop-3.x-yellow?logo=apache-hadoop)
![Spark](https://img.shields.io/badge/Spark-3.x-orange?logo=apache-spark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue?logo=postgresql)
![PowerBI](https://img.shields.io/badge/PowerBI-Data-yellow?logo=powerbi)
![Docker](https://img.shields.io/badge/Docker-Container-blue?logo=docker)

---

## 📚 Mục lục

- [Giới thiệu dự án](#giới-thiệu-dự-án)
- [Tổng quan hệ thống](#tổng-quan-hệ-thống)
  - [1.1 Thu thập dữ liệu](#11-thu-thập-dữ-liệu)
  - [1.2 Lưu trữ dữ liệu thô](#12-lưu-trữ-dữ-liệu-thô)
  - [1.3 Xử lý dữ liệu](#13-xử-lý-dữ-liệu)
  - [1.4 Lập lịch tự động](#14-lập-lịch-tự-động)
- [Kết quả đạt được](#kết-quả-đạt-được)
- [Phân tích dữ liệu](#phân-tích-dữ-liệu)
  - [3.1 Phân tích tổng quan](#31-phân-tích-tổng-quan)
  - [3.2 Phân tích chi tiết](#32-phân-tích-chi-tiết)
---

## Giới thiệu dự án

**Xây dựng đường ống dữ liệu thu thập MetaData từ GooglePlay Store**  
- Xây dựng luồng dữ liệu từ crawl → transform → load → visualize  
- Giám sát, lập lịch tác vụ bằng Airflow
- Triển khai hệ thống bằng Docker

---

## Tổng quan hệ thống

<img width="768" height="391" alt="image" src="https://github.com/user-attachments/assets/7699c17c-2fb4-4892-b78d-093b13c2c32d" />

### 1.1 Thu thập dữ liệu

<img width="1109" height="325" alt="image" src="https://github.com/user-attachments/assets/fa099df1-3325-4a7b-9e5f-fb5a9a53b381" />

- Crawl dữ liệu từ GooglePlay Store:
  - Lấy URL gốc: `https://play.google.com/store/games`
  - Thêm tham số query để vào các danh mục cụ thể
  - Lấy tất cả link trỏ tới các game của lần crawl
  - Công nghệ: Requests và BeautifulSoup4

### 1.2 Lưu trữ dữ liệu thô

- Dữ liệu thô: DataFrame gồm 2 cột: `Url` và `content` (HTML)
- Lưu ở định dạng parquet trên Hadoop

<img width="1180" height="368" alt="image" src="https://github.com/user-attachments/assets/95c371cf-6140-44b3-ba7d-af8c9219701b" />
<img width="1349" height="636" alt="image" src="https://github.com/user-attachments/assets/c96acb13-6c96-48a7-b7b8-946cd5c2cb01" />

### 1.3 Xử lý dữ liệu

- Xử lý theo lô từng ngày bằng Spark, làm sạch với UDF
- Đo kiểm hiệu suất Spark Cluster chế độ Standalone

<img width="1250" height="548" alt="image" src="https://github.com/user-attachments/assets/02fabf7b-04a8-4248-84f5-e2fb51171a04" />

### 1.4 Lập lịch tự động

- Tự động hóa với Airflow

<img width="1114" height="561" alt="image" src="https://github.com/user-attachments/assets/85909d8d-c663-4d9f-9679-85048332fed7" />
<img width="1082" height="254" alt="image" src="https://github.com/user-attachments/assets/d69735f8-3a55-455e-9e18-05565da54b68" />

---

## Kết quả đạt được

- **Kho Dữ Liệu:**

  <img width="1147" height="696" alt="image" src="https://github.com/user-attachments/assets/f980360d-aa13-4a8d-8da5-f735b26a0c8b" />

- **Trực quan hóa bằng PowerBI:**

  <img width="1297" height="733" alt="image" src="https://github.com/user-attachments/assets/790c09c7-6f49-4335-b650-61bc269630e7" />

---

## Phân tích dữ liệu

Đã crawl được **466 ứng dụng** từ **380 công ty** với **43 thể loại**.

### 3.1 Phân tích tổng quan

- Theo công ty: Google, Meta, SuperCell,... những gã khổng lồ công nghệ dẫn đầu thị trường
- Theo danh mục: Communication, Social, Photography, Action, Productivity dẫn đầu về lượt tải, đánh giá, bình luận
- Theo ứng dụng: Facebook, Google Meet, Whatsapp, Messenger,... thống trị top đầu
- Theo độ tuổi: "Everyone" và "Teen" chiếm 2/3 số app

### 3.2 Phân tích chi tiết

#### Phân tích 1

<img width="1297" height="662" alt="image" src="https://github.com/user-attachments/assets/8925e7c4-85a4-4eb8-be86-185d4f3f7309" />

- **Google LLC**: 27 tỷ lượt download, rating TB 4.35, sở hữu 7 app ở 2 danh mục chính (Communication, Photography)
- Công ty phát triển rất ít ứng dụng nhưng tập trung rất mạnh vào nghiên cứu các sản phẩm chính => Họ **quan tâm chất lượng hơn là số lượng**
- Các sản phẩm đều **hướng tới "Everyone"** => Đều là các ứng dụng phục vụ cho nhu cầu đời sống như: Giao tiếp và chỉnh sửa ảnh.
- Lượt **phản hồi rất thấp** tuy nhiên **Rating TB** thì **cao** => Một lượng lớn các **khách hàng trung thành** và có độ **hài lòng** nhất định với các ứng dụng.
#### Phân tích 2

**Data:**

<img width="1275" height="735" alt="image" src="https://github.com/user-attachments/assets/c1c900df-3dd4-4dc6-a00f-8d3d134a0710" />

**Context:**

- Quan sát Phân bổ theo **"Danh mục"** phát hiện ra danh mục **"Action"**: Download & review **cao**, rating **thấp**
- Hiệu suất phản hồi/app cao
- App cho tuổi **Teen** chiếm đa số ở thể loại này

> Thể loại **"Action"** rất hấp dẫn khi có lượt **Download cao**, tuy nhiên vấn đề là lượt **review cao** nhưng **Rating TB lại thấp**
> => Khả năng cao toàn là bình luận góp ý tiêu cực mãnh liệt. Điều này hoàn toàn phù hợp khi đối tượng sử dụng là các bạn tuổi "Teen", một độ tuổi rất "nóng" và chơi một thể loại game "Hành Động" cũng rất cảm xúc. Có thể chơi thua cũng sẽ vào góp một bình luận => **lượt review cao**, chơi thua xóa game sau đó lại tải lại => **lượt download cao**. Chứng tỏ nhóm **"Teen"** là một nhóm **khách hàng trung thành tiềm năng**


**Action**
- Nhà phát triển cần khắc phục vấn đề tiêu cực, cập nhật app để nâng cao trải nghiệm người dùng tốt hơn.
