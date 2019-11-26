
select date, deviceID
                ,countif(cate1 is null) as null_cate
                ,countif(cate1 like ('Ô Tô - Xe Máy - Xe Đạp%')) as transportation_cate
                ,countif(cate1 like ('Làm Đẹp - Sức Khỏe%')) as beauty_health_cate
                ,countif(cate1 like ('Nhà Cửa - Đời Sống%')) as home_living_cate
                ,countif(cate1 like ('Bách Hóa Online%')) as accessories_cate
                ,countif(cate1 like ('Voucher - Dịch vụ%')) as voucher_cate
                ,countif(cate1 like ('Điện Tử - Điện Lạnh%')) as electronic_cate
                ,countif(cate1 like ('Nhà Sách Tiki%')) as book_cate
                ,countif(cate1 like ('Điện Thoại - Máy Tính Bảng%')) as phone_tablet_cate
                ,countif(cate1 like ('Dịch Vụ%')) as service_cate
                ,countif(cate1 like ('Thời Trang%')) as lifestyle_cate
                ,countif(cate1 like ('Phiếu đặt cọc%')) as phieu_dat_coc_cate
                ,countif(cate1 like ('Thiết Bị Số - Phụ Kiện Số%')) as digital_device_cate
                ,countif(cate1 like ('Thể Thao - Dã Ngoại%')) as sport_cate
                ,countif(cate1 like ('Hàng Quốc Tế%')) as cross_border_cate
                ,countif(cate1 like ('Laptop - Máy Vi Tính - Linh kiện%')) as laptop_cate
                ,countif(cate1 like ('Máy Ảnh - Máy Quay Phim%')) as camera_cate
                ,countif(cate1 like ('Đồ Chơi - Mẹ & Bé%')) as mom_baby_cate
                ,countif(cate1 like ('Recycle Bin%')) as recycle_bin_cate
                ,countif(cate1 like ('Điện Gia Dụng%')) as electric_appliances_cate
                ,countif(cate1 like ('Chương Trình Khuyến Mãi%')) as promotion_cate
from `consumer_product.feature_pdp`
group by 1, 2