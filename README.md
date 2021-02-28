<b>PySpark là gì?</b>

Apache Spark là một khung tính toán cụm nhanh được sử dụng để xử lý, truy vấn và phân tích dữ liệu lớn. Dựa trên tính toán trong bộ nhớ, nó có lợi thế hơn một số khung dữ liệu lớn khác.

<b>Vậy, Spark DataFrame là gì?</b>

Ngày xửa, ngày xưa, khi Spark ver 1.3 ra đời, Spark đã đẻ thêm tính năng có tên là Spark DataFrame.

Có thể thiết lập Schema cho Spark RDD và có thể tạo Object DataFrame.
Thế bạn thao tác dữ liệu chỉ sử dụng RDD thấy gặp vấn đề gì phức tạp không ?
Viết code có khó khăn không ? Rồi vấn đề về hiệu năng ?

Giống như viết SQL, đầy đủ chức năng như select, where ... đặc biệt là join với các DataFrame khác. Sử dụng các method như filter, select để trích xuất dữ liệu theo cột, hàng. Xử gọn các loại data như Log ... với groupBy → agg
Thêm 1 cột dễ dàng với UDF(User Defined Function)
Giống như SQL, Spark DataFrame đã hỗ trợ Pivot (Spark 1.6 trở lên) rất hữu ích cho việc lập bảng biểu, báo cáo.
Tóm lại là dễ xài, đơn giản hơn RDD mà hiệu suất, khả năng optimized truy vấn tốt hơn RDD.
Chính vì vậy với các trường hợp thông thường, các bạn nên xài DataFrame.

<b>Cơ chế hoạt động của spark</b>

Bước đầu tiên để thiết lập spark là tạo ra 1 cụm xử lý (cluster) trên một máy chủ. Cụm xử lý này được kết nối tới rất nhiều nodes khác nhau. Máy chủ (master) sẽ làm nhiệm vụ: phân chia dữ liệu và quản lý tính toán trên các máy con. máy chủ sẽ kết nối đến các máy con (slaves) trong cụm bằng các session. Máy chủ sẽ gửi dữ liệu và yêu cầu tính toán để máy con thực thi. Sau khi có kết quả máy con có nhiệm vụ trả về máy chủ. Máy chủ tổng hợp tất cả các tính toán trên máy con để tính ra kết quả cuối cùng.

Trong bài này do mới làm quen với spark nên mình sẽ khởi tạo một cluster trên local. Thay vì kết nối tới những máy khác, các tính toán sẽ được thực hiện chỉ trên server local thông qua một giả lập cụm.

<b>Khái niệm mapreduce:</b>

Mapreduce là một mô hình lập trình, thực hiện quá tình xử lý tập dữ liệu lớn. Mapreduce gồm 2 pha : map và reduce.
Hàm Map : Các xử lý một cặp (key, value) để sinh ra một cặp (keyI, valueI) - key và value trung gian. Dữ liệu này input vào hàm Reduce.
Hàm Reduce : Tiếp nhận các (keyI, valueI) và trộn các cặp (keyI, valueI) trung gian , lấy ra các valueI có cùng keyI.

Việc của lập trình viên là quan tâm tới 2 hàm Map và Reduce. Còn các vấn đề khác như : phân chia các dữ liệu đầu vào, lịch trình thực thi các machines, handling các machines failure, quản lý việc giao tiếp giữa các machines là việc của hệ thống run-time.
=> Lập trình viên có thể không có kinh nghiệm về hệ thống song song và phân tán vẫn dễ dàng vận hành một hệ thống phân tán lớn.
Áp dụng mô hình MapReduce chạy trên lượng lớn các machine cỡ hàng ngàn machine và data lên đến mức Terabytes.

*Các job sau dễ dàng sử dụng Mapreduce:
Thống kê số từ khóa xuất hiện trong các documents.
Thống kê số documents có chứa từ khóa.
Thống kê số câu match với pattern trong các documents.
Thống kê số URLs xuất hiện trong các web pages.
Thống kê số lượt truy cập các URLs.
Thống kê số từ khóa trên các hostnames.
Distributed Sort.

<b>Hoạt động của mapreduce: </b>

*Ý tưởng
  Chia vấn đề cần xử lý thành các phần nhỏ để xử lý.
  Xử lý các phần nhỏ đó một cách song song và độc lập trên các máy tính phân tán.
  Tổng hợp các kết quả thu được để dưa ra kết quả cuối cùng.
*Hoạt động của MapReduce có thể được tóm tắt như sau:
  Đọc dữ liệu đầu vào
  Xử lý dữ liệu đầu vào (thực hiện hàm map)
  Sắp xếp và trộn các kết quả thu được từ các máy tính phân tán thích hợp nhất.
  Tổng hợp các kết quả trung gian thu được ( thực hiện hàm reduce)
  Đưa ra kết quả cuối cùng.
