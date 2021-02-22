**PySpark là gì?

Apache Spark là một khung tính toán cụm nhanh được sử dụng để xử lý, truy vấn và phân tích dữ liệu lớn. Dựa trên tính toán trong bộ nhớ, nó có lợi thế hơn một số khung dữ liệu lớn khác.

**Vậy, Spark DataFrame là gì?

Ngày xửa, ngày xưa, khi Spark ver 1.3 ra đời, Spark đã đẻ thêm tính năng có tên là Spark DataFrame.

Có thể thiết lập Schema cho Spark RDD và có thể tạo Object DataFrame.
Thế bạn thao tác dữ liệu chỉ sử dụng RDD thấy gặp vấn đề gì phức tạp không ?
Viết code có khó khăn không ? Rồi vấn đề về hiệu năng ?

Giống như viết SQL, đầy đủ chức năng như select, where ... đặc biệt là join với các DataFrame khác. Sử dụng các method như filter, select để trích xuất dữ liệu theo cột, hàng. Xử gọn các loại data như Log ... với groupBy → agg
Thêm 1 cột dễ dàng với UDF(User Defined Function)
Giống như SQL, Spark DataFrame đã hỗ trợ Pivot (Spark 1.6 trở lên) rất hữu ích cho việc lập bảng biểu, báo cáo.
Tóm lại là dễ xài, đơn giản hơn RDD mà hiệu suất, khả năng optimized truy vấn tốt hơn RDD.
Chính vì vậy với các trường hợp thông thường, các bạn nên xài DataFrame.

**Cơ chế hoạt động của spark

Bước đầu tiên để thiết lập spark là tạo ra 1 cụm xử lý (cluster) trên một máy chủ. Cụm xử lý này được kết nối tới rất nhiều nodes khác nhau. Máy chủ (master) sẽ làm nhiệm vụ: phân chia dữ liệu và quản lý tính toán trên các máy con. máy chủ sẽ kết nối đến các máy con (slaves) trong cụm bằng các session. Máy chủ sẽ gửi dữ liệu và yêu cầu tính toán để máy con thực thi. Sau khi có kết quả máy con có nhiệm vụ trả về máy chủ. Máy chủ tổng hợp tất cả các tính toán trên máy con để tính ra kết quả cuối cùng.

Trong bài này do mới làm quen với spark nên mình sẽ khởi tạo một cluster trên local. Thay vì kết nối tới những máy khác, các tính toán sẽ được thực hiện chỉ trên server local thông qua một giả lập cụm.

**Khái niệm mapreduce:

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
**Hoạt động của mapreduce:

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
  
  Pyspark properties
Thuộc tính Spark
Thuộc tính Spark kiểm soát hầu hết các cài đặt ứng dụng và được cấu hình riêng cho từng ứng dụng. Các thuộc tính này có thể được đặt trực tiếp trên SparkConf được chuyển đến của bạn SparkContext. SparkConfcho phép bạn định cấu hình một số thuộc tính chung (ví dụ: URL chính và tên ứng dụng), cũng như các cặp khóa-giá trị tùy ý thông qua set()phương thức. Ví dụ, chúng ta có thể khởi tạo một ứng dụng với hai luồng như sau:
Lưu ý rằng chúng tôi chạy với local [2], nghĩa là hai luồng - thể hiện sự song song “tối thiểu”, có thể giúp phát hiện lỗi chỉ tồn tại khi chúng tôi chạy trong bối cảnh phân tán.
 
Lưu ý rằng chúng ta có thể có nhiều hơn 1 luồng ở chế độ cục bộ và trong những trường hợp như Spark Streaming, chúng tôi thực sự có thể yêu cầu nhiều hơn 1 luồng để ngăn chặn bất kỳ loại vấn đề chết đói nào.
Các thuộc tính chỉ định một số khoảng thời gian nên được cấu hình với một đơn vị thời gian. Định dạng sau được chấp nhận:
 
Thuộc tính chỉ định kích thước byte phải được cấu hình với đơn vị kích thước. Định dạng sau được chấp nhận:
 
Trong khi các số không có đơn vị thường được hiểu là byte, một số ít được hiểu là KiB hoặc MiB. Xem tài liệu về các thuộc tính cấu hình riêng lẻ. Việc chỉ định đơn vị là mong muốn nếu có thể.
Thuộc tính có sẵn
Hầu hết các thuộc tính kiểm soát cài đặt nội bộ đều có giá trị mặc định hợp lý. Một số tùy chọn phổ biến nhất để đặt là:
Thuộc tính ứng dụng

 
 
 
 
 
 
 
 
 

Ngoài những thuộc tính này, các thuộc tính sau cũng có sẵn và có thể hữu ích trong một số trường hợp:
Môi trường thực thi
 
       
 


Tìm hiểu về pyspark RDD
RDD (Tập dữ liệu phân tán có khả năng phục hồi) là gì?
RDD (Tập dữ liệu phân tán có khả năng phục hồi) là một khối xây dựng cơ bản của PySpark, là bộ sưu tập đối tượng phân tán không thay đổi, chịu được lỗi. Ý nghĩa bất biến khi bạn tạo RDD, bạn không thể thay đổi nó. Mỗi bản ghi trong RDD được chia thành các phân vùng logic, có thể được tính toán trên các nút khác nhau của cụm. 
Nói cách khác, RDD là một tập hợp các đối tượng tương tự như danh sách trong Python, với sự khác biệt là RDD được tính toán trên một số quy trình nằm rải rác trên nhiều máy chủ vật lý còn được gọi là các nút trong một cụm trong khi tập hợp Python tồn tại và xử lý chỉ trong một quy trình.
Ngoài ra, RDD cung cấp sự trừu tượng hóa dữ liệu của việc phân vùng và phân phối dữ liệu được thiết kế để chạy tính toán song song trên một số nút, trong khi thực hiện các phép biến đổi trên RDD, chúng ta không phải lo lắng về tính song song như PySpark cung cấp theo mặc định.
Hướng dẫn Apache PySpark RDD này mô tả các thao tác cơ bản có sẵn trên RDDs, chẳng hạn như  map(), filter()và  persist()và nhiều hơn nữa. Ngoài ra, hướng dẫn này cũng giải thích các hàm RDD Ghép nối hoạt động trên RDD của các cặp khóa-giá trị như  groupByKey() và join()v.v.
Lưu ý: RDD có thể có tên và số nhận dạng duy nhất (id)
Lợi ích của PySpark RDD
PySpark được thích nghi rộng rãi trong cộng đồng Học máy và Khoa học dữ liệu do những ưu điểm của nó so với lập trình python truyền thống.
Xử lý trong bộ nhớ
PySpark tải dữ liệu từ đĩa và xử lý trong bộ nhớ và giữ dữ liệu trong bộ nhớ, đây là điểm khác biệt chính giữa PySpark và Mapreduce (I / O chuyên sâu). Giữa các lần biến đổi, chúng ta cũng có thể lưu cache / duy trì RDD trong bộ nhớ để sử dụng lại các tính toán trước đó.
Tạo RDD
RDD được tạo ra chủ yếu theo hai cách khác nhau,song song hóa một bộ sưu tập hiện có vàtham khảo một tập dữ liệu trong một hệ thống bên ngoài lưu trữ ( HDFS, S3và nhiều hơn nữa). 
Trước khi chúng ta xem xét các ví dụ, trước tiên hãy khởi tạo SparkSession bằng cách sử dụng phương thức mẫu xây dựng được định nghĩa trong lớp SparkSession. Trong khi khởi tạo, chúng ta cần cung cấp tên chính và ứng dụng như hình bên dưới. Trong ứng dụng thời gian thực, bạn sẽ vượt qua master từ spark-submit thay vì hardcoding trên ứng dụng Spark
 
master() - Nếu bạn đang chạy nó trên cụm, bạn cần sử dụng tên chính của mình làm đối số cho chủ (). thông thường, nó sẽ là một trong hai  yarn (Yet Another Resource Negotiator)hoặc  mesos tùy thuộc vào thiết lập cụm của bạn.
Sử dụng local[x]khi chạy ở chế độ Độc lập. x phải là một giá trị nguyên và phải lớn hơn 0; điều này thể hiện số lượng phân vùng nó sẽ tạo khi sử dụng RDD, DataFrame và Dataset. Tốt nhất, giá trị x phải là số lõi CPU bạn có.
appName() - Được sử dụng để đặt tên ứng dụng của bạn.
getOrCreate() - Điều này trả về một đối tượng SparkSession nếu đã tồn tại, tạo một đối tượng mới nếu chưa tồn tại.
Tạo RDD bằng sparkContext.parallelize ()
Bằng cách sử dụng parallelize()hàm của SparkContext ( sparkContext.parallelize () ), bạn có thể tạo RDD. Hàm này tải bộ sưu tập hiện có từ chương trình trình điều khiển của bạn vào song song hóa RDD. Đây là phương pháp cơ bản để tạo RDD và được sử dụng khi bạn đã có dữ liệu trong bộ nhớ được tải từ tệp hoặc từ cơ sở dữ liệu. và nó yêu cầu tất cả dữ liệu phải có trên chương trình trình điều khiển trước khi tạo RDD.
 
Tạo RDD bằng sparkContext.textFile ()
Sử dụng phương thức textFile (), chúng ta có thể đọc tệp văn bản (.txt) vào RDD.
 Tạo RDD bằng sparkContext.wholeTextFiles ()
Hàm wholeTextFiles () trả về một PairRDD với khóa là đường dẫn tệp và giá trị là nội dung tệp.
 Bên cạnh việc sử dụng các tệp văn bản, chúng ta cũng có thể tạo RDD từ tệp CSV , JSON và nhiều định dạng khác.
Tạo RDD trống bằng sparkContext.emptyRDD
Sử dụng emptyRDD()phương thức trên sparkContext, chúng ta có thể  tạo một RDD không có dữ liệu . Phương pháp này tạo ra một RDD trống không có phân vùng.
 Tạo RDD trống với phân vùng
Đôi khi, chúng ta có thể cần ghi RDD trống vào các tệp theo phân vùng, Trong trường hợp này, bạn nên tạo RDD trống có phân vùng.
 
RDD Song song hóa
Khi chúng tôi sử dụng parallelize()hoặc textFile()hoặc  wholeTextFiles()các phương thức của SparkContxt để khởi tạo RDD, nó sẽ tự động chia dữ liệu thành các phân vùng dựa trên tính khả dụng của tài nguyên. khi bạn chạy nó trên máy tính xách tay, nó sẽ tạo các phân vùng có cùng số lượng lõi có sẵn trên hệ thống của bạn.
getNumPartitions () - Đây là một hàm RDD trả về một số phân vùng mà tập dữ liệu của chúng tôi được chia thành.
 Đặt song song theo cách thủ công - Chúng ta cũng có thể đặt một số phân vùng theo cách thủ công, tất cả những gì chúng ta cần là chuyển một số phân vùng làm tham số thứ hai cho các hàm này chẳng hạn   sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10).
Tìm hiểu về Pyspark dataframe
PySpark - Tạo DataFrame với các ví dụ
Bạn có thể Tạo một PySpark DataFrame bằng cách sử dụng toDF()và createDataFrame()các phương pháp, cả hai hàm này đều lấy các chữ ký khác nhau để tạo DataFrame từ RDD, danh sách và DataFrame hiện có.
Bạn cũng có thể tạo PySpark DataFrame từ các nguồn dữ liệu như TXT, CSV, JSON, ORV, Avro, Parquet, định dạng XML bằng cách đọc từ hệ thống tệp HDFS, S3, DBFS, Azure Blob, v.v.
Cuối cùng, PySpark DataFrame cũng có thể được tạo bằng cách đọc dữ liệu từ Cơ sở dữ liệu RDBMS và Cơ sở dữ liệu NoSQL.
1. Tạo DataFrame từ RDD
Một cách dễ dàng để tạo PySpark DataFrame là từ một RDD hiện có. đầu tiên, hãy tạo một Spark RDD từ một Danh sách bộ sưu tập bằng cách gọi hàm song song () từ SparkContext . Chúng tôi sẽ cần đối tượng rdd này cho tất cả các ví dụ của chúng tôi bên dưới.
 1.1 Sử dụng hàm toDF ()
Phương thức toDF () của PySpark RDD được sử dụng để tạo DataFrame từ RDD hiện có. Vì RDD không có  Nếu bạn muốn cung cấp tên cột cho toDF() phương pháp sử dụng DataFrame với tên cột làm đối số như hình dưới đây.
 Điều này tạo ra lược đồ của DataFrame với các tên cột.

 
Theo mặc định, kiểu dữ liệu của các cột này suy ra kiểu dữ liệu. Chúng ta có thể thay đổi hành vi này bằng cách cung cấp lược đồ , trong đó chúng ta có thể chỉ định tên cột, kiểu dữ liệu và giá trị có thể làm trống cho mỗi trường / cột.
1.2 Sử dụng createDataFrame () từ SparkSession
Sử dụng createDataFrame () từ SparkSession là một cách khác để tạo và nó lấy đối tượng rdd làm đối số. và chuỗi với toDF () để chỉ định tên cho các cột.
 
2. Tạo DataFrame từ Bộ sưu tập danh sách
Trong phần này, chúng ta sẽ xem cách tạo PySpark DataFrame từ một danh sách. Những ví dụ này sẽ tương tự như những gì chúng ta đã thấy trong phần trên với RDD, nhưng chúng ta sử dụng đối tượng dữ liệu danh sách thay vì đối tượng “rdd” để tạo DataFrame.
2.1 Sử dụng createDataFrame () từ SparkSession
Gọi createDataFrame()from SparkSession là một cách khác để tạo PySpark DataFrame, nó lấy một đối tượng danh sách làm đối số. và chuỗi với toDF()để chỉ định tên cho các cột.
 2.2 Sử dụng createDataFrame () với kiểu Hàng
createDataFrame()có một chữ ký khác trong PySpark lấy bộ sưu tập kiểu Hàng và lược đồ cho tên cột làm đối số. Để sử dụng điều này, trước tiên chúng ta cần chuyển đổi đối tượng “dữ liệu” từ danh sách sang danh sách Hàng.
 2.3 2.3 Tạo DataFrame bằng lược đồ
Nếu bạn muốn chỉ định tên cột cùng với kiểu dữ liệu của chúng, trước tiên bạn nên tạo lược đồ StructType và sau đó gán nó trong khi tạo DataFrame.

 Điều này dẫn đến sản lượng thấp hơn.
 
3. Tạo DataFrame từ các nguồn Dữ liệu
Trong thời gian thực, hầu hết bạn tạo DataFrame từ các tệp nguồn dữ liệu như CSV, Văn bản, JSON, XML, v.v.
PySpark theo mặc định hỗ trợ nhiều định dạng dữ liệu mà không cần nhập bất kỳ thư viện nào và để tạo DataFrame, bạn cần sử dụng phương pháp thích hợp có sẵn trong DataFrameReaderlớp.
3.1 Tạo DataFrame từ CSV
Sử dụng csv()phương thức của DataFrameReaderđối tượng để tạo DataFrame từ tệp CSV. bạn cũng có thể cung cấp các tùy chọn như dấu phân cách sẽ sử dụng, cho dù bạn đã trích dẫn dữ liệu, định dạng ngày tháng, lược đồ suy luận, v.v. Vui lòng tham khảo PySpark Read CSV thành DataFrame
 
3.2. Tạo từ tệp văn bản (TXT)
Tương tự, bạn cũng có thể tạo DataFrame bằng cách đọc từ tệp Văn bản, sử dụng text()phương thức của DataFrameReader để làm như vậy.
 3.3. Tạo từ tệp JSON
PySpark cũng được sử dụng để xử lý các tệp dữ liệu bán cấu trúc như định dạng JSON. bạn có thể sử dụng json()phương thức của DataFrameReader để đọc tệp JSON vào DataFrame. Dưới đây là một ví dụ đơn giản.


 Tương tự, chúng ta có thể tạo DataFrame trong PySpark từ hầu hết các cơ sở dữ liệu quan hệ mà tôi chưa trình bày ở đây và tôi sẽ để bạn khám phá.
4. Các nguồn khác (Avro, Parquet, ORC, Kafka)
Có thể tạo DataFrame bằng cách đọc các tệp Avro, Parquet, ORC, Binary và truy cập bảng Hive và HBase, đồng thời đọc dữ liệu từ Kafka mà tôi đã giải thích trong các bài viết dưới đây, tôi khuyên bạn nên đọc chúng khi có thời gian.

