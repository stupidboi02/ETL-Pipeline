**Project: Xây dựng đường ống dữ liệu thu thập MetaData từ GooglePlay Store**
**Mục tiêu:**
- Xây dựng luồng dữ liệu từ crawl -> transform -> load -> visualize
- Giám sát, lập lịch tác vụ bằng Airflow
  
***1. Tổng quan hệ thống***
  <img width="779" height="402" alt="image" src="https://github.com/user-attachments/assets/25a744e8-47bd-4283-beb6-d8322e9e7f2d" />
  
**1.1 Thu thập dữ liệu**
   <img width="1109" height="325" alt="image" src="https://github.com/user-attachments/assets/fa099df1-3325-4a7b-9e5f-fb5a9a53b381" />

   Dữ liệu được Crawl từ trang chủ của GooglePlay Store như sau:
   - Lấy Url gốc ban đầu là "https://play.google.com/store/games". Sau đó tiến hành thêm các tham số query vào Url để vào được các danh mục cụ thể
   - Trong các trang này ta tiến hành lấy tất cả đường link trỏ tới tất cả các game của lần crawl đó
   - Công nghệ sử dụng: Requests và Bs4
     
**1.2 Lưu trữ dữ liệu thô**
  Dữ liệu thô ở đây chính là các dataframe bao gồm 2 cột: "Url" và "content" - dạng html của url đó
  <img width="1180" height="368" alt="image" src="https://github.com/user-attachments/assets/95c371cf-6140-44b3-ba7d-af8c9219701b" />
- Dữ liệu thô được lưu ở định dạng parquet tại Hadoop:
  <img width="1349" height="636" alt="image" src="https://github.com/user-attachments/assets/c96acb13-6c96-48a7-b7b8-946cd5c2cb01" />

  
**1.3 Xử lý dữ liệu**
  - Dữ liệu được xử lý từng ngày theo lô bằng Spark: Sử dụng các hàm UDF để làm sạch
  Kiểm tra tốc độ xử lý của Spark Cluster ở chế độ Standalone như phía dưới:
<img width="1250" height="548" alt="image" src="https://github.com/user-attachments/assets/02fabf7b-04a8-4248-84f5-e2fb51171a04" />

**1.4 Lập lịch tự động**
<img width="1114" height="561" alt="image" src="https://github.com/user-attachments/assets/85909d8d-c663-4d9f-9679-85048332fed7" />
<img width="1082" height="254" alt="image" src="https://github.com/user-attachments/assets/d69735f8-3a55-455e-9e18-05565da54b68" />
***2. Các kết quả đạt được***
   - **Kho Dữ Liệu**
     <img width="1147" height="696" alt="image" src="https://github.com/user-attachments/assets/f980360d-aa13-4a8d-8da5-f735b26a0c8b" />
   - **Trực quan hóa bằng PowerBi**
     <img width="1297" height="733" alt="image" src="https://github.com/user-attachments/assets/790c09c7-6f49-4335-b650-61bc269630e7" />
**3. Phân tích**
- Tổng thể đến hiện tại đã crawl được 466 ứng dụng từ 380 công ty khác nhau. Các ứng dụng thuộc vào 43 thể loại
**3.1 Phân tích tổng quan**
  - Theo công ty: Các công ty đang áp đảo thị trường bao gồm các gã khổng lồ như Google, Meta, SuperCell,..
  - Theo danh mục: Các ứng dụng thuộc thể loại Communication, Social, Photography, Action, Producity vẫn đang dẫn đầu về lượt tải với điểm đánh giá và lượt bình luận rất cao
  - Theo ứng dụng: Các ứng dụng Thuộc hệ sinh thái của Google và Meta đều chiếm lĩnh top 1 như: Facebook, Google Meet, Whatsapp, Messenger,...
  - Theo độ tuổi yêu cầu: Các ứng dụng cho "Everyone" và "Teen" chiếm 2/3 số lượng các ứng dụng.
**3.2 Phân tích tìm kiếm**
    **Phân tích số 1.**
    <img width="1297" height="662" alt="image" src="https://github.com/user-attachments/assets/8925e7c4-85a4-4eb8-be86-185d4f3f7309" />
    - Công ty Google LLC đang dẫn đầu với 27 tỷ lượt download với ratings trung bình 4.35. Hiện tại đang sở hữu 7 Ứng Dụng chủ yếu ở 2 danh mục Communication và Photography. Hiệu suất phản hồi trên lượt download rất nhỏ nhưng ratings cao chứng tỏ có một lượng khách hàng lớn trung thành rất ít lời review phàn nàn.
      => Công ty vô cùng đầu tư vào nghiên cứu, phát triển các sản phẩm chủ lực của họ ở các lĩnh vực "giao tiếp" và "chỉnh ảnh", ít nhưng chất lượng.
      **Phân tích số 2.**
      **Data**
<img width="1275" height="735" alt="image" src="https://github.com/user-attachments/assets/c1c900df-3dd4-4dc6-a00f-8d3d134a0710" />
      **Context**
- Quan sát thấy từ biểu đồ phân bổ theo danh mục,
  - Thể loại "Action" Có lượt download và reviews cao tuy nhiên trung bình ratings lại thấp.
  - Hiệu suất phản hồi trên mỗi App của thể loại này là cực cao
  - Số lượng App dành cho tuổi Teen ở danh mục này chiếm đa phần tỉ trọng
=> Có thể thấy Thể loại "Action" rất hấp dẫn mang lại số lượng download tốt, lượt reviews cao mà ratings lại thấp, => có vấn đề. Khả năng cao là các lượt reviews đều là các lời phàn nàn về ứng dụng. Đây là các ứng dụng thuộc thể loại "Hành động" tức là các tựa game chủ yếu thu hút các bạn tuổi "Teen". Mà độ tuổi "Teen" rất khó làm hài lòng, họ có thể phàn nàn về một lý do rất bình thường cho một ứng dụng.
**Action**
- Đối với nhà phát triển: Cố gắng khắc phục các vấn đề tiêu cực, đưa ra bản cập nhật để mang lại trải nghiệm tốt hơn




      

