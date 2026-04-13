"""
step_08_check_distribution.py
-----------------------------
Pipeline Step 8: Kiểm tra phân phối dữ liệu (Data Distribution).

Đầu vào : PipelineConfig.TRAIN_READY_FILE (step7_train_ready.csv)
Nhiệm vụ:
  1. Đọc tập dữ liệu TRAIN-READY cuối cùng.
  2. Phân loại 4 nhãn cảm xúc dựa trên ngưỡng 0.5 của Valence và Energy.
  3. Thống kê số lượng, tỷ lệ phần trăm của từng nhãn.
  4. Cảnh báo tự động nếu phát hiện mất cân bằng dữ liệu nghiêm trọng.
"""

import os
import pandas as pd
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger

# Định nghĩa tên 4 góc phần tư cảm xúc (Quadrants)
QUAD_NAMES = {
    0: 'Buồn - Êm dịu (Low V, Low E)',
    1: 'Căng - Sôi động (Low V, High E)',
    2: 'Vui - Êm dịu (High V, Low E)',
    3: 'Vui - Sôi động (High V, High E)'
}

def load_dataset(filepath, logger):
    """Đọc file CSV an toàn."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Không tìm thấy '{filepath}'. "
            "Hãy chắc chắn Bước 7 đã chạy thành công."
        )
    
    df = pd.read_csv(filepath, low_memory=False)
    logger.info(f"[LOAD] Đọc thành công {len(df):,} bài hát từ '{os.path.basename(filepath)}'.")
    return df

def analyze_distribution(df, logger):
    """Phân loại cảm xúc và vẽ biểu đồ text vào log."""
    if 'valence' not in df.columns or 'energy' not in df.columns:
        raise ValueError("[!] Dataset thiếu cột 'valence' hoặc 'energy' để phân loại.")

    # Đảm bảo kiểu dữ liệu là số
    df['valence'] = pd.to_numeric(df['valence'], errors='coerce')
    df['energy']  = pd.to_numeric(df['energy'], errors='coerce')

    # Dọn dẹp nếu có dòng bị thiếu feature
    before = len(df)
    df = df.dropna(subset=['valence', 'energy'])
    if len(df) < before:
        logger.warning(f"[CLEAN] Đã bỏ qua {before - len(df)} bài bị thiếu giá trị Valence/Energy.")

    # Tính toán Quadrant (0, 1, 2, 3)
    # Công thức: (Valence >= 0.5)*2 + (Energy >= 0.5)
    df['quadrant'] = (df['valence'] >= 0.5).astype(int) * 2 + (df['energy'] >= 0.5).astype(int)

    # Đếm số lượng
    counts = df['quadrant'].value_counts().sort_index()
    total = counts.sum()

    logger.info("=" * 65)
    logger.info(f"{'THỐNG KÊ PHÂN PHỐI 4 CUNG BẬC CẢM XÚC':^65}")
    logger.info("=" * 65)

    max_cnt = counts.max()
    
    # In ra báo cáo và biểu đồ bar dạng text
    for q in range(4):
        cnt = counts.get(q, 0)
        pct = (cnt / total) * 100 if total > 0 else 0
        
        # Scale thanh bar tối đa 20 block để log không bị tràn
        bar_length = int((cnt / max_cnt) * 20) if max_cnt > 0 else 0
        bar = '█' * bar_length
        
        logger.info(f"[-] {QUAD_NAMES[q]:<35}: {cnt:5d} bài ({pct:5.1f}%) | {bar}")

    # Hệ thống cảnh báo mất cân bằng (Imbalance Warning)
    max_pct = (counts.max() / total) * 100
    min_pct = (counts.min() / total) * 100

    logger.info("-" * 65)
    if max_pct > 40 or min_pct < 10:
        logger.warning("[!] CẢNH BÁO: DỮ LIỆU ĐANG BỊ MẤT CÂN BẰNG NGHIÊM TRỌNG")
        logger.warning(f"    -> Nhóm áp đảo nhất chiếm tới {max_pct:.1f}%")
        logger.warning(f"    -> Nhóm thiểu số nhất chỉ có {min_pct:.1f}%")
        logger.warning("    => Lời khuyên: Hãy sử dụng `WeightedRandomSampler` trong PyTorch ")
        logger.warning("       để bù đắp trọng số khi huấn luyện mô hình TAMS-MER.")
    else:
        logger.info("[OK] Tuyệt vời! Dữ liệu phân phối tương đối cân bằng.")

    return counts

def main():
    logger = get_logger("Step08_CheckDistribution", "step8.log")
    input_file = str(PipelineConfig.TRAIN_READY_FILE)

    logger.info("=" * 65)
    logger.info("BƯỚC 8: KIỂM TRA TỶ LỆ DỮ LIỆU ĐẦU RA")
    logger.info("=" * 65)

    try:
        df = load_dataset(input_file, logger)
        analyze_distribution(df, logger)
        
        logger.info("=" * 65)
        logger.info("[THÀNH CÔNG] ĐÃ HOÀN TẤT TOÀN BỘ PIPELINE CHUẨN BỊ DỮ LIỆU!")
        logger.info("=" * 65)
        
    except Exception as e:
        logger.error(f"[!] Tiến trình bị ngắt do lỗi: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Đã dừng chương trình (Ctrl+C).")